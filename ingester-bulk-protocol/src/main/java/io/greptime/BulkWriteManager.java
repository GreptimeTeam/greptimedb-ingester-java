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
import io.greptime.rpc.TlsOptions;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.BulkFlightClient;
import org.apache.arrow.flight.BulkFlightClient.ClientStreamListener;
import org.apache.arrow.flight.BulkFlightClient.PutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
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
            long allocationLimit = SystemPropertyUtil.getLong(Keys.FLIGHT_ALLOCATION_LIMIT, 1024 * 1024 * 1024);
            BufferAllocator rootAllocator = new RootAllocator(new FlightAllocationListener(), allocationLimit);

            // Add a shutdown hook to close the root allocator when the JVM exits
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    AutoCloseables.close(rootAllocator);
                } catch (Exception e) {
                    LOG.error("Failed to close root allocator", e);
                }
            }));

            return rootAllocator;
        }
    }

    private static BufferAllocator getRootAllocator() {
        return RootAllocatorHolder.ROOT_ALLOCATOR;
    }

    private final Endpoint endpoint;
    private final BulkFlightClient flightClient;
    private final BufferAllocator allocator;

    private BulkWriteManager(Endpoint endpoint, BulkFlightClient flightClient, BufferAllocator allocator) {
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
     * @param compressionType the compression type to use for arrow messages
     * @param tlsOptions the TLS options for the Flight client
     * @return a BulkWriteManager instance
     */
    public static BulkWriteManager create(
            Endpoint endpoint,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            ArrowCompressionType compressionType,
            TlsOptions tlsOptions) {
        Location location = Location.forGrpcInsecure(endpoint.getAddr(), endpoint.getPort());

        String allocatorName = String.format("BufferAllocator(%s)", location);
        BufferAllocator allocator =
                getRootAllocator().newChildAllocator(allocatorName, allocatorInitReservation, allocatorMaxAllocation);
        Ensures.ensureNonNull(
                allocator,
                "Failed to create child buffer allocator, initReservation: %s, maxAllocation: %s",
                allocatorInitReservation,
                allocatorMaxAllocation);

        BulkFlightClient flightClient = BulkFlightClient.builder()
                .location(location)
                .allocator(allocator)
                .compressionType(compressionType)
                .tlsOptions(tlsOptions)
                .build();
        BulkWriteManager client = new BulkWriteManager(endpoint, flightClient, allocator);

        LOG.info("BulkWriteManager created: {}", client);

        return client;
    }

    /**
     * @see #intoBulkWriteStream(String, String, Schema, long, CallOption...)
     */
    public BulkWriteService intoBulkWriteStream(
            String database, String table, Schema schema, long timeoutMs, CallOption... options) {
        return intoBulkWriteStream(database, table, schema, timeoutMs, new AsyncPutListener(), options);
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
    public BulkWriteService intoBulkWriteStream(
            String database,
            String table,
            Schema schema,
            long timeoutMs,
            PutListener metadataListener,
            CallOption... options) {
        FlightDescriptor descriptor = FlightDescriptor.path(database, table);
        return new BulkWriteService(this, schema, descriptor, metadataListener, timeoutMs, options);
    }

    VectorSchemaRoot createSchemaRoot(Schema schema) {
        return VectorSchemaRoot.create(schema, this.allocator);
    }

    ClientStreamListener startPut(
            FlightDescriptor descriptor, PutListener metadataListener, Runnable onReadyHandler, CallOption... options) {
        return this.flightClient.startPut(descriptor, metadataListener, onReadyHandler, options);
    }

    DictionaryProvider newDefaultDictionaryProvider() {
        return this.flightClient.newDefaultDictionaryProvider();
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
            LOG.debug("onAllocation: {}", size);
            ALLOCATION_BYTES.inc(size);
        }

        @Override
        public void onRelease(long size) {
            LOG.debug("onRelease: {}", size);
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
