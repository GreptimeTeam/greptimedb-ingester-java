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

import com.google.protobuf.ByteString;
import io.greptime.common.TimeoutCompletableFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.flight.BulkFlightClient.ClientStreamListener;
import org.apache.arrow.flight.BulkFlightClient.PutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
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

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final BulkWriteManager manager;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final ClientStreamListener listener;
    private final OnReadyHandler onReadyHandler;
    private final AsyncPutListener metadataListener;
    private final long timeoutMs;

    public BulkWriteService(
            BulkWriteManager manager,
            BufferAllocator allocator,
            Schema schema,
            FlightDescriptor descriptor,
            long timeoutMs,
            CallOption... options) {
        this.manager = manager;
        this.allocator = allocator;
        this.root = manager.createSchemaRoot(schema);
        this.onReadyHandler = new OnReadyHandler();
        this.metadataListener = new AsyncPutListener();
        this.listener = manager.startPut(descriptor, this.metadataListener, this.onReadyHandler, options);
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
    public boolean isStreamReady() {
        return this.listener.isReady();
    }

    /**
     * Send the current contents of the associated {@link VectorSchemaRoot}.
     *
     * <p>This will not necessarily block until the message is actually sent; it may buffer messages
     * in memory. Use {@link #isStreamReady()} to check if there is backpressure and avoid excessive buffering.
     *
     * @return a PutStage object that contains the stream ready future, the write result future and the number of in-flight requests.
     */
    public PutStage putNext() {
        CompletableFuture<Boolean> streamReadyFuture = this.onReadyHandler.nextReadyFuture();
        long id = this.idGenerator.incrementAndGet();
        byte[] metadata = new Metadata.RequestMetadata(id).toJsonBytesUtf8();
        try (ArrowBuf metadataBuf = this.allocator.buffer(metadata.length)) {
            metadataBuf.setBytes(0, metadata);
            IdentifiableCompletableFuture writeResultFuture = new IdentifiableCompletableFuture(id, this.timeoutMs);
            this.metadataListener.attach(id, writeResultFuture);
            writeResultFuture.whenComplete((r, t) -> {
                if (t != null) {
                    // The stream ready future cannot catch the exception, so we need to complete it here.
                    streamReadyFuture.completeExceptionally(t);
                }
            });
            this.listener.putNext(metadataBuf);
            return new PutStage(streamReadyFuture, writeResultFuture, this.metadataListener.numInFlight());
        } finally {
            this.root.clear();
        }
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

    public static class PutStage {
        private final CompletableFuture<Boolean> streamReadyFuture;
        private final CompletableFuture<Integer> writeResultFuture;
        private final int numInFlight;

        public PutStage(
                CompletableFuture<Boolean> streamReadyFuture,
                CompletableFuture<Integer> writeResultFuture,
                int numInFlight) {
            this.streamReadyFuture = streamReadyFuture;
            this.writeResultFuture = writeResultFuture;
            this.numInFlight = numInFlight;
        }

        public CompletableFuture<Boolean> streamReadyFuture() {
            return this.streamReadyFuture;
        }

        public CompletableFuture<Integer> writeResultFuture() {
            return this.writeResultFuture;
        }

        public int numInFlight() {
            return this.numInFlight;
        }
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

        public CompletableFuture<Boolean> nextReadyFuture() {
            TimeoutCompletableFuture<Boolean> future =
                    new TimeoutCompletableFuture<Boolean>(BulkWriteService.this.timeoutMs, TimeUnit.MILLISECONDS);
            if (this.futureRef.compareAndSet(null, future)) {
                future.scheduleTimeout();
                return future;
            }
            // If CAS failed, we just return a completed future with the current status,
            // maybe the stream is not ready yet.
            return CompletableFuture.completedFuture(BulkWriteService.this.listener.isReady());
        }
    }

    static class IdentifiableCompletableFuture extends TimeoutCompletableFuture<Integer> {
        private final long id;

        public IdentifiableCompletableFuture(long id, long timeoutMs) {
            super(timeoutMs, TimeUnit.MILLISECONDS);
            this.id = id;
        }

        public long getId() {
            return this.id;
        }
    }

    class AsyncPutListener implements PutListener {
        private final ConcurrentMap<Long, IdentifiableCompletableFuture> futuresInFlight;
        private final CompletableFuture<Void> completed;

        AsyncPutListener() {
            this.futuresInFlight = new ConcurrentHashMap<>();
            this.completed = new CompletableFuture<>();
            this.completed.whenComplete((r, t) -> {
                if (t != null) {
                    // Also complete all the futures with the same exception
                    for (IdentifiableCompletableFuture future : this.futuresInFlight.values()) {
                        future.completeExceptionally(t);
                    }
                }
                // When completed, clear the futuresInFlight
                this.futuresInFlight.clear();
            });
        }

        public void attach(long id, IdentifiableCompletableFuture future) {
            future.whenComplete((r, t) -> {
                // Remove the future from the map when it's completed
                this.futuresInFlight.remove(id);
            });
            this.futuresInFlight.put(id, future);
        }

        public int numInFlight() {
            return this.futuresInFlight.size();
        }

        @Override
        public void onNext(PutResult val) {
            try (ArrowBuf metadata = val.getApplicationMetadata()) {
                if (metadata == null) {
                    return;
                }
                String metadataString =
                        ByteString.copyFrom(metadata.nioBuffer()).toStringUtf8();
                Metadata.ResponseMetadata responseMetadata = Metadata.ResponseMetadata.fromJson(metadataString);
                IdentifiableCompletableFuture future = this.futuresInFlight.get(responseMetadata.getRequestId());
                if (future != null) {
                    future.complete(responseMetadata.getAffectedRows());
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            this.completed.completeExceptionally(StatusUtils.fromThrowable(t));
        }

        @Override
        public final void onCompleted() {
            this.completed.complete(null);
        }

        @Override
        public boolean isCancelled() {
            return this.completed.isDone();
        }

        @Override
        public void getResult() {
            try {
                this.completed.get();
            } catch (InterruptedException e) {
                throw StatusUtils.fromThrowable(e);
            } catch (ExecutionException e) {
                throw StatusUtils.fromThrowable(e.getCause());
            }
        }
    }
}
