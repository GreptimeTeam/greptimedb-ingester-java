/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime;

import com.codahale.metrics.Counter;
import io.greptime.common.Endpoint;
import io.greptime.common.Keys;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.MetricsUtil;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightClient.PutListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkWriteManager is a specialized manager for efficiently writing block data to the server.
 *
 * It encapsulates a Flight client and a buffer allocator to manage memory resources.
 *
 * The primary function of this manager is to establish bulk write streams,
 * which provide an optimized channel for transmitting block data to the server.
 * These streams handle the serialization and transfer of data in an efficient manner.
 *
 * <p>
 * `ZERO_COPY_WRITE` is disabled by default, if you want to enable it, you can set the system property
 * `arrow.flight.enable_zero_copy_write` to `true`.
 * </p>
 */
public class BulkWriteManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteManager.class);

    // Lazy initialization of the root allocator
    private static class RootAllocatorHolder {

        private static final BufferAllocator ROOT_ALLOCATOR = createRootAllocator();

        private static BufferAllocator createRootAllocator() {
            // max allocation size in bytes
            long allocationLimit = SystemPropertyUtil.getLong(Keys.FLIGHT_ALLOCATION_LIMIT, Long.MAX_VALUE);
            BufferAllocator rootAllocator = new RootAllocator(new FlightAllocationListener(), allocationLimit);

            // Add a shutdown hook to close the root allocator when the JVM exits
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    AutoCloseables.close(rootAllocator);
                } catch (Exception e) {
                    LOG.error("Failed to close root buffer allocator", e);
                }
            }));

            return rootAllocator;
        }
    }

    private static BufferAllocator getRootAllocator() {
        return RootAllocatorHolder.ROOT_ALLOCATOR;
    }

    private final Endpoint endpoint;
    private final FlightClient flightClient;
    private final BufferAllocator allocator;

    private BulkWriteManager(Endpoint endpoint, FlightClient flightClient, BufferAllocator allocator) {
        this.endpoint = Ensures.ensureNonNull(endpoint, "null `endpoint`");
        this.flightClient = Ensures.ensureNonNull(flightClient, "null `flightClient`");
        this.allocator = Ensures.ensureNonNull(allocator, "null `allocator`");
    }

    /**
     * Creates a bulk write manager for efficiently writing data to the server.
     *
     * @param endpoint the endpoint of the server
     * @param allocatorInitReservation the initial space reservation (obtained from this allocator)
     * @param allocatorMaxAllocation the maximum amount of space the new child allocator can allocate
     * @return a BulkWriteManager instance
     */
    public static BulkWriteManager create(
            Endpoint endpoint, long allocatorInitReservation, long allocatorMaxAllocation) {
        Location location = Location.forGrpcInsecure(endpoint.getAddr(), endpoint.getPort());

        String allocatorName = String.format("BufferAllocator(%s)", location);
        BufferAllocator allocator =
                getRootAllocator().newChildAllocator(allocatorName, allocatorInitReservation, allocatorMaxAllocation);
        Ensures.ensureNonNull(
                allocator,
                "Failed to create child buffer allocator, initReservation: %s, maxAllocation: %s",
                allocatorInitReservation,
                allocatorMaxAllocation);

        FlightClient flightClient =
                FlightClient.builder().location(location).allocator(allocator).build();
        BulkWriteManager client = new BulkWriteManager(endpoint, flightClient, allocator);

        LOG.info("BulkWriteManager created: {}", client);

        return client;
    }

    /**
     * @see #bulkWriteStream(String, String, Schema, long, CallOption...)
     */
    public BulkWriteService bulkWriteStream(
            String database, String table, Schema schema, long timeoutMs, CallOption... options) {
        return this.bulkWriteStream(database, table, schema, timeoutMs, new AsyncPutListener(), options);
    }

    /**
     * Creates a bulk write stream for efficiently writing data to the server.
     *
     * @param database the name of the target database
     * @param table the name of the target table
     * @param schema the Arrow schema defining the structure of the data to be written
     * @param timeoutMs the timeout in milliseconds for the write operation
     * @param metadataListener listener for handling server metadata responses during the write operation
     * @param options optional RPC-layer hints to configure the underlying Flight client call
     * @return a BulkStreamWriter instance that manages the data transfer process
     */
    public BulkWriteService bulkWriteStream(
            String database,
            String table,
            Schema schema,
            long timeoutMs,
            PutListener metadataListener,
            CallOption... options) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, this.allocator);
        FlightDescriptor descriptor = FlightDescriptor.path(database, table);
        ClientStreamListener listener = this.flightClient.startPut(descriptor, root, metadataListener, options);
        return new BulkWriteService(root, listener, timeoutMs);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(this.flightClient, this.allocator);
    }

    @Override
    public String toString() {
        return "BulkWriteManager{" + "endpoint=" + endpoint + ", flightClient=" + flightClient + ", allocator="
                + allocator + '}';
    }

    static class FlightAllocationListener implements AllocationListener {

        static final Counter ALLOCATION_BYTES = MetricsUtil.counter("flight_allocation_bytes");

        @Override
        public void onAllocation(long size) {
            ALLOCATION_BYTES.inc(size);
        }

        @Override
        public void onRelease(long size) {
            ALLOCATION_BYTES.dec(size);
        }

        @Override
        public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
            LOG.warn("onFailedAllocation: {} {}", size, outcome);
            return false;
        }

        @Override
        public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
            LOG.info("onChildAdded: {} {}", parentAllocator, childAllocator);
        }

        @Override
        public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
            LOG.info("onChildRemoved: {} {}", parentAllocator, childAllocator);
        }
    }
}
