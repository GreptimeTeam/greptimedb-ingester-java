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

import io.greptime.common.TimeoutCompletableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.flight.BulkFlightClient.ClientStreamListener;
import org.apache.arrow.flight.BulkFlightClient.PutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * BulkWriteService is a specialized class for efficiently writing data to the server in bulk operations.
 *
 * It encapsulates two key components:
 * 1. A ClientStreamListener that manages and controls the data upload process
 * 2. A VectorSchemaRoot that contains the structured data to be transmitted
 *
 * This service handles the serialization and transfer of Arrow-formatted data in an optimized manner,
 * providing a streamlined interface for bulk write operations.
 */
public class BulkWriteService implements AutoCloseable {
    private final BulkWriteManager manager;
    private final VectorSchemaRoot root;
    private final ClientStreamListener listener;
    private final OnReadyHandler onReadyHandler;
    private final long timeoutMs;

    public BulkWriteService(
            BulkWriteManager manager,
            Schema schema,
            FlightDescriptor descriptor,
            PutListener metadataListener,
            long timeoutMs,
            CallOption... options) {
        this.manager = manager;
        this.root = manager.createSchemaRoot(schema);
        this.onReadyHandler = new OnReadyHandler();
        this.listener = manager.startPut(descriptor, metadataListener, this.onReadyHandler, options);
        this.timeoutMs = timeoutMs;
    }

    public void start() {
        this.listener.start(this.root, this.manager.newDefaultDictionaryProvider());
    }

    public void start(IpcOption ipcOption) {
        this.listener.start(this.root, this.manager.newDefaultDictionaryProvider(), ipcOption);
    }

    /**
     * Get the root of the bulk stream.
     *
     * VectorSchemaRoot the root containing data.
     *
     * @return root
     */
    public VectorSchemaRoot getRoot() {
        return this.root;
    }

    /**
     * Try to use zero copy write.
     */
    public void tryUseZeroCopyWrite() {
        this.listener.setUseZeroCopy(true);
    }

    /**
     * Check if the stream is ready to send the next message.
     *
     * @return true if the stream is ready to send the next message, false otherwise
     */
    public boolean isReady() {
        return this.listener.isReady();
    }

    /**
     * Send the current contents of the associated {@link VectorSchemaRoot}.
     *
     * <p>This will not necessarily block until the message is actually sent; it may buffer messages
     * in memory. Use {@link #isReady()} to check if there is backpressure and avoid excessive buffering.
     *
     * @return a future that completes with true if the message was sent, false otherwise. Note that this future
     *         may be delayed in completion or may never complete if all executor threads on the server are busy,
     *         or the RPC method body is implemented in a blocking fashion.
     */
    public CompletableFuture<Boolean> putNext() {
        // The future may be null if the previous `putNext()` is not completed.
        TimeoutCompletableFuture<Boolean> future = this.onReadyHandler.nextFuture();
        this.listener.putNext();
        this.root.clear();
        // If the future is null, we just return a completed future with the current ready state.
        return future == null ? CompletableFuture.completedFuture(isReady()) : future.scheduleTimeout();
    }

    /**
     * Complete the bulk write operation. Indicate that transmission is finished.
     */
    public void completed() {
        this.listener.completed();
    }

    /**
     * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
     * happened during the upload.
     */
    public void waitServerCompleted() {
        this.listener.getResult();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(this.root, this.manager);
    }

    class OnReadyHandler implements Runnable {
        private final AtomicReference<TimeoutCompletableFuture<Boolean>> futureRef = new AtomicReference<>();

        @Override
        public void run() {
            TimeoutCompletableFuture<Boolean> future = this.futureRef.getAndSet(null);
            if (future != null) {
                future.complete(BulkWriteService.this.listener.isReady());
            }
        }

        public TimeoutCompletableFuture<Boolean> nextFuture() {
            TimeoutCompletableFuture<Boolean> future =
                    new TimeoutCompletableFuture<Boolean>(BulkWriteService.this.timeoutMs, TimeUnit.MILLISECONDS);
            if (this.futureRef.compareAndSet(null, future)) {
                return future;
            }
            return null;
        }
    }
}
