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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.flight.BulkFlightClient.ClientStreamListener;
import org.apache.arrow.flight.BulkFlightClient.PutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteService.class);

    private final AtomicLong idGenerator = new AtomicLong(0);

    private final BulkWriteManager manager;
    private final BufferAllocator allocator;
    private final VectorSchemaRoot root;
    private final ClientStreamListener listener;
    private final AsyncPutListener metadataListener;
    private final String tableName;
    private final long timeoutMs;

    /**
     * Constructs a new BulkWriteService.
     *
     * @param manager The BulkWriteManager that manages this service
     * @param allocator The BufferAllocator for memory management
     * @param schema The Arrow schema defining the data structure
     * @param descriptor The FlightDescriptor identifying the data stream
     * @param timeoutMs The timeout in milliseconds for operations
     * @param maxRequestsInFlight the max in-flight requests in the stream
     * @param options Additional call options for the Flight client
     */
    public BulkWriteService(
            BulkWriteManager manager,
            BufferAllocator allocator,
            Schema schema,
            FlightDescriptor descriptor,
            long timeoutMs,
            int maxRequestsInFlight,
            CallOption... options) {
        this.manager = manager;
        this.allocator = allocator;
        this.root = manager.createSchemaRoot(schema);
        this.metadataListener = new AsyncPutListener();
        this.listener = manager.startPut(descriptor, this.metadataListener, maxRequestsInFlight, timeoutMs, options);
        this.tableName = diagnosticName(descriptor);
        this.timeoutMs = timeoutMs;
    }

    static String diagnosticName(FlightDescriptor descriptor) {
        if (descriptor.isCommand() || descriptor.getPath().isEmpty()) {
            return "unknown";
        }
        return String.join("/", descriptor.getPath());
    }

    /**
     * Starts the bulk write stream with default IPC options.
     */
    public void start() {
        LOG.debug("Starting bulk write stream with default IPC options");
        this.listener.start(this.root, this.manager.newDefaultDictionaryProvider());
    }

    /**
     * Starts the bulk write stream with custom IPC options.
     *
     * @param ipcOption The IPC options to use for the Arrow data transfer
     */
    public void start(IpcOption ipcOption) {
        LOG.debug("Starting bulk write stream with custom IPC options: {}", ipcOption);
        this.listener.start(this.root, this.manager.newDefaultDictionaryProvider(), ipcOption);
    }

    /**
     * Gets the VectorSchemaRoot of the bulk stream.
     *
     * @return The VectorSchemaRoot containing the data to be written
     */
    public VectorSchemaRoot getRoot() {
        return this.root;
    }

    /**
     * Enables zero-copy write mode for improved performance.
     * This avoids unnecessary memory copies when sending data.
     */
    public void tryUseZeroCopyWrite() {
        LOG.info("Enabling zero-copy write mode for improved performance");
        this.listener.setUseZeroCopy(true);
    }

    /**
     * Checks if the stream is ready to send the next message.
     *
     * @return true if the stream is ready to send the next message, false otherwise
     */
    public boolean isStreamReady() {
        return this.listener.isReady();
    }

    /**
     * Sends the current contents of the associated VectorSchemaRoot to the server.
     * This method will:
     * 1. Generate a unique ID for the request
     * 2. Create a future to track the operation
     * 3. Prepare and send the data with metadata
     * 4. Clear the VectorSchemaRoot for the next batch
     *
     * @return A PutStage object containing the future and the number of in-flight requests
     */
    public PutStage putNext() {
        long id = nextId();
        int totalRowCount = this.root.getRowCount();

        LOG.debug("Starting putNext operation [id={}], total row count: {}", id, totalRowCount);

        // Create future with timeout and attach to listener
        IdentifiableCompletableFuture future =
                new IdentifiableCompletableFuture(id, this.timeoutMs, this.tableName, totalRowCount, LOG);
        future.whenComplete((r, t) -> {
            if (t instanceof TimeoutCompletableFuture.FutureDeadlineExceededException) {
                abort(newTimeoutFailure("Timed out waiting for a bulk write response", t));
            }
        });
        ArrowBuf metadataBuf = null;
        Throwable putFailure = null;
        try {
            if (!this.metadataListener.attach(id, future)) {
                return new PutStage(future, this.metadataListener.numInFlight());
            }

            // Prepare metadata buffer
            byte[] metadata = new Metadata.RequestMetadata(id).toJsonBytesUtf8();
            metadataBuf = this.allocator.buffer(metadata.length);
            metadataBuf.writeBytes(metadata);

            // Send data to the server
            LOG.debug("Sending data to server [id={}]", id);
            this.listener.putNext(metadataBuf);
            metadataBuf = null; // Ownership transfers to the Flight writer.

            int inFlightCount = this.metadataListener.numInFlight();
            LOG.debug("Data sent successfully [id={}], in-flight requests: {}", id, inFlightCount);

            return new PutStage(future, inFlightCount);
        } catch (RuntimeException | Error e) {
            Throwable failure = e;
            if (!future.completeExceptionally(e)) {
                try {
                    future.join();
                } catch (CompletionException completedFailure) {
                    if (completedFailure.getCause() != null) {
                        failure = completedFailure.getCause();
                    }
                }
            }
            putFailure = failure;
            // Flight closes metadata after accepting ownership; a positive refcount here means the
            // failure occurred before that handoff completed and this method still owns the buffer.
            if (metadataBuf != null && metadataBuf.refCnt() > 0) {
                try {
                    metadataBuf.close();
                } catch (RuntimeException closeError) {
                    failure.addSuppressed(closeError);
                }
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new CompletionException(failure);
        } finally {
            // Clear the root to prepare for next batch
            try {
                this.root.clear();
                LOG.debug("Cleared root for next batch [id={}], previous row count: {}", id, totalRowCount);
            } catch (RuntimeException | Error clearError) {
                if (putFailure == null) {
                    throw clearError;
                }
                putFailure.addSuppressed(clearError);
            }
        }
    }

    /**
     * Completes the bulk write operation, indicating that transmission is finished.
     * This signals to the server that no more data will be sent.
     */
    public void completed() {
        LOG.info("Completing bulk write operation, signaling end of transmission");
        this.listener.completed();
    }

    /**
     * Waits for the stream to finish processing on the server side.
     * This method must be called to be notified of any errors that may have
     * occurred during the upload process.
     */
    public void waitServerCompleted() {
        LOG.info("Waiting for server to complete processing");
        try {
            this.metadataListener.getResult(this.timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            FlightRuntimeException timeout = newTimeoutFailure("Timed out waiting for bulk stream completion", e);
            abort(timeout);
            throw timeout;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            FlightRuntimeException interrupted = CallStatus.CANCELLED
                    .withDescription("Interrupted while waiting for bulk stream completion")
                    .withCause(e)
                    .toRuntimeException();
            abort(interrupted);
            throw interrupted;
        } catch (ExecutionException e) {
            throw StatusUtils.fromThrowable(e.getCause());
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing BulkWriteService resources");
        if (!this.metadataListener.isDone()) {
            abort(CallStatus.CANCELLED
                    .withDescription("Bulk write stream closed before completion")
                    .toRuntimeException());
        }
        AutoCloseables.close(this.root, this.manager);
    }

    void abort(Throwable failure) {
        this.metadataListener.onError(failure);
        try {
            this.listener.cancel("Bulk write stream aborted", failure);
        } catch (RuntimeException | Error cancelFailure) {
            failure.addSuppressed(cancelFailure);
        }
    }

    private static FlightRuntimeException newTimeoutFailure(String description, Throwable cause) {
        return CallStatus.TIMED_OUT
                .withDescription(description)
                .withCause(cause)
                .toRuntimeException();
    }

    private long nextId() {
        long id;
        do {
            id = this.idGenerator.incrementAndGet();
        } while (id == 0); // Skip ID 0 as it's reserved for special cases
        return id;
    }

    /**
     * Represents the state of a put operation, containing both the future for tracking
     * completion and the current count of in-flight requests.
     */
    public static class PutStage {
        private final CompletableFuture<Integer> future;
        private final int numInFlight;

        /**
         * Creates a new PutStage.
         *
         * @param future The future that will be completed when the operation finishes
         * @param numInFlight The current number of in-flight requests
         */
        public PutStage(CompletableFuture<Integer> future, int numInFlight) {
            this.future = future;
            this.numInFlight = numInFlight;
        }

        /**
         * Gets the future that will be completed when the operation finishes.
         *
         * @return The CompletableFuture for this operation
         */
        public CompletableFuture<Integer> future() {
            return this.future;
        }

        /**
         * Gets the number of in-flight requests at the time this PutStage was created.
         *
         * @return The count of in-flight requests
         */
        public int numInFlight() {
            return this.numInFlight;
        }

        /**
         * Logs diagnostic context when a caller times out waiting for this request.
         *
         * @param timeout the timeout raised to the caller
         * @param value the caller's timeout value
         * @param unit the caller's timeout unit
         */
        public void logTimeout(Throwable timeout, long value, TimeUnit unit) {
            if (this.future instanceof IdentifiableCompletableFuture) {
                ((IdentifiableCompletableFuture) this.future).logTimeout(timeout, unit.toMillis(value));
            }
        }
    }

    /**
     * A CompletableFuture with an associated ID for tracking purposes.
     * Extends TimeoutCompletableFuture to support operation timeouts.
     */
    static class IdentifiableCompletableFuture extends TimeoutCompletableFuture<Integer> {
        private final long id;
        private final long timeoutMs;
        private final String tableName;
        private final int rows;
        private final Logger logger;
        private final AtomicBoolean timeoutLogged = new AtomicBoolean();

        /**
         * Creates a new IdentifiableCompletableFuture.
         *
         * @param id The unique identifier for this future
         * @param timeoutMs The timeout in milliseconds
         */
        public IdentifiableCompletableFuture(long id, long timeoutMs) {
            this(id, timeoutMs, "unknown", 0, LOG);
        }

        IdentifiableCompletableFuture(long id, long timeoutMs, String tableName, int rows, Logger logger) {
            super(timeoutMs, TimeUnit.MILLISECONDS);
            this.id = id;
            this.timeoutMs = timeoutMs;
            this.tableName = tableName;
            this.rows = rows;
            this.logger = logger;
        }

        /**
         * Gets the unique identifier for this future.
         *
         * @return The ID of this future
         */
        public long getId() {
            return this.id;
        }

        @Override
        public Integer get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            try {
                return super.get(timeout, unit);
            } catch (TimeoutException e) {
                logTimeout(e, unit.toMillis(timeout));
                throw e;
            }
        }

        void logTimeout(Throwable timeout, long timeoutMs) {
            if (this.timeoutLogged.compareAndSet(false, true)) {
                this.logger.warn(
                        "Bulk write timed out - table={}, request-id={}, rows={}, timeout={}ms",
                        this.tableName,
                        this.id,
                        this.rows,
                        timeoutMs,
                        timeout);
            }
        }
    }

    /**
     * Listener for handling asynchronous responses from the server during bulk write operations.
     * Manages the lifecycle of in-flight requests and their associated futures.
     */
    static class AsyncPutListener implements PutListener {
        private final ConcurrentMap<Long, IdentifiableCompletableFuture> futuresInFlight;
        private final CompletableFuture<Void> completed;
        private final Object terminalLock;
        private boolean terminal;
        private Throwable terminalFailure;

        /**
         * Creates a new AsyncPutListener.
         */
        AsyncPutListener() {
            this.futuresInFlight = new ConcurrentHashMap<>();
            this.completed = new CompletableFuture<>();
            this.terminalLock = new Object();
        }

        /**
         * Attaches a future to this listener for tracking.
         *
         * @param id The unique identifier for the request
         * @param future The future to track
         * @return true if the future was attached, false if the stream was already terminal
         */
        public boolean attach(long id, IdentifiableCompletableFuture future) {
            Throwable failure;
            synchronized (this.terminalLock) {
                if (this.terminal) {
                    failure = this.terminalFailure;
                } else {
                    this.futuresInFlight.put(id, future);
                    failure = null;
                }
            }

            if (failure != null) {
                future.completeExceptionally(failure);
                return false;
            }

            future.whenComplete((r, t) -> {
                // Remove the future from the map when it's completed
                this.futuresInFlight.remove(id, future);

                if (t != null) {
                    if (t instanceof TimeoutCompletableFuture.FutureDeadlineExceededException) {
                        future.logTimeout(t, future.timeoutMs);
                    } else {
                        LOG.error("Put operation failed [id={}]: {}", id, t.getMessage(), t);
                        // If a put next operation fails, we complete the future with the exception
                        // and the stream will be terminated immediately to prevent further operations
                        onError(t);
                    }
                } else {
                    LOG.debug("Put operation succeeded [id={}], affected rows: {}", id, r);
                }
            });

            future.scheduleTimeout();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Attached future [id={}], current in-flight count: {}", id, this.futuresInFlight.size());
            }
            return true;
        }

        /**
         * Gets the number of currently in-flight requests.
         *
         * @return The count of in-flight requests
         */
        public int numInFlight() {
            return this.futuresInFlight.size();
        }

        @Override
        public void onNext(PutResult val) {
            ArrowBuf metadata = val.getApplicationMetadata();
            if (metadata == null) {
                LOG.warn("Received PutResult with null metadata");
                return;
            }
            String metadataString = ByteString.copyFrom(metadata.nioBuffer()).toStringUtf8();
            Metadata.ResponseMetadata responseMetadata = Metadata.ResponseMetadata.fromJson(metadataString);

            long requestId = responseMetadata.getRequestId();
            int affectedRows = responseMetadata.getAffectedRows();

            LOG.debug("Received response [id={}], affected rows: {}", requestId, affectedRows);

            IdentifiableCompletableFuture future = this.futuresInFlight.get(requestId);
            if (future != null) {
                future.complete(affectedRows);
            } else if (requestId != 0) { // 0 is reserved for special cases
                LOG.warn("A timeout response [id={}] finally received", requestId);
            }
        }

        @Override
        public void onError(Throwable t) {
            Throwable failure = StatusUtils.fromThrowable(t);
            if (terminate(failure, false)) {
                LOG.error("Stream error occurred: {}", t.getMessage(), t);
            }
        }

        @Override
        public final void onCompleted() {
            if (terminate(null, true)) {
                LOG.info("Server signaled stream completion");
            }
        }

        private boolean terminate(Throwable failure, boolean normalCompletion) {
            List<IdentifiableCompletableFuture> pending;
            Throwable completionFailure;
            synchronized (this.terminalLock) {
                if (this.terminal) {
                    return false;
                }

                pending = new ArrayList<>(this.futuresInFlight.values());
                this.futuresInFlight.clear();
                if (normalCompletion && pending.isEmpty()) {
                    completionFailure = null;
                    this.terminalFailure = new IllegalStateException("The bulk write stream is already completed");
                } else if (failure != null) {
                    completionFailure = failure;
                    this.terminalFailure = failure;
                } else {
                    completionFailure = new IllegalStateException(
                            "The bulk write stream completed before all put responses were received");
                    this.terminalFailure = completionFailure;
                }
                this.terminal = true;
            }

            if (completionFailure != null) {
                this.completed.completeExceptionally(completionFailure);
                for (IdentifiableCompletableFuture future : pending) {
                    future.completeExceptionally(completionFailure);
                }
            } else {
                this.completed.complete(null);
            }
            return true;
        }

        @Override
        public boolean isCancelled() {
            return this.completed.isCancelled();
        }

        @Override
        public boolean isCompletedExceptionally() {
            return this.completed.isCompletedExceptionally();
        }

        boolean isDone() {
            return this.completed.isDone();
        }

        void getResult(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            this.completed.get(timeout, unit);
        }

        @Override
        public void getResult() {
            try {
                this.completed.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw StatusUtils.fromThrowable(e);
            } catch (ExecutionException e) {
                throw StatusUtils.fromThrowable(e.getCause());
            }
        }
    }
}
