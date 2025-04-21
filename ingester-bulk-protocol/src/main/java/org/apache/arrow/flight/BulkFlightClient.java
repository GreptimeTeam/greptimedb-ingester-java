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

package org.apache.arrow.flight;

import com.codahale.metrics.Timer;
import io.greptime.ArrowCompressionType;
import io.greptime.common.util.MetricsUtil;
import io.greptime.rpc.TlsOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLException;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.grpc.ClientInterceptorAdapter;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;

/**
 * Client for Flight services.
 *
 * This class is a modified version of the original {@link FlightClient},
 * with some changes to support bulk write.
 */
public class BulkFlightClient implements AutoCloseable {
    /** The maximum number of trace events to keep on the gRPC Channel. This value disables channel tracing. */
    private static final int MAX_CHANNEL_TRACE_EVENTS = 0;

    private final BufferAllocator allocator;
    private final ManagedChannel channel;

    private final FlightServiceStub asyncStub;
    private final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor;
    private final List<FlightClientMiddleware.Factory> middleware;

    private final ArrowCompressionType compressionType;

    /**
     * Create a Flight client from an allocator and a gRPC channel.
     */
    BulkFlightClient(
            BufferAllocator incomingAllocator,
            ManagedChannel channel,
            List<FlightClientMiddleware.Factory> middleware,
            ArrowCompressionType compressionType) {
        this.allocator = incomingAllocator.newChildAllocator("bulk-flight-client", 0, Long.MAX_VALUE);
        this.channel = channel;
        this.middleware = middleware;
        this.compressionType = compressionType;
        ClientInterceptor[] interceptors = new ClientInterceptor[] {new ClientInterceptorAdapter(middleware)};

        // Create a channel with interceptors pre-applied for DoGet and DoPut
        Channel interceptedChannel = ClientInterceptors.intercept(channel, interceptors);

        this.asyncStub = FlightServiceGrpc.newStub(interceptedChannel);
        this.doPutDescriptor = FlightBindingService.getDoPutDescriptor(this.allocator);
    }

    /**
     * Add a middleware to the client.
     *
     * @param factory The factory to add.
     */
    public void addClientMiddleware(FlightClientMiddleware.Factory factory) {
        this.middleware.add(factory);
    }

    /**
     * Create or append a descriptor with another stream.
     *
     * @param descriptor FlightDescriptor the descriptor for the data
     * @param root VectorSchemaRoot the root containing data
     * @param metadataListener A handler for metadata messages from the server. This will be passed buffers that will be
     *     freed after {@link StreamListener#onNext(Object)} is called!
     * @param options RPC-layer hints for this call.
     * @return ClientStreamListener an interface to control uploading data
     */
    public ClientStreamListener startPut(
            FlightDescriptor descriptor, VectorSchemaRoot root, PutListener metadataListener, CallOption... options) {
        return startPut(descriptor, root, new MapDictionaryProvider(), metadataListener, options);
    }

    /**
     * Create or append a descriptor with another stream.
     *
     * @param descriptor FlightDescriptor the descriptor for the data
     * @param root VectorSchemaRoot the root containing data
     * @param provider A dictionary provider for the root.
     * @param metadataListener A handler for metadata messages from the server.
     * @param options RPC-layer hints for this call.
     * @return ClientStreamListener an interface to control uploading data.
     *     {@link ClientStreamListener#start(VectorSchemaRoot, DictionaryProvider)} will already have been called.
     */
    public ClientStreamListener startPut(
            FlightDescriptor descriptor,
            VectorSchemaRoot root,
            DictionaryProvider provider,
            PutListener metadataListener,
            CallOption... options) {
        Preconditions.checkNotNull(root, "root must not be null");
        Preconditions.checkNotNull(provider, "provider must not be null");
        ClientStreamListener writer = startPut(descriptor, metadataListener, options);
        writer.start(root, provider);
        return writer;
    }

    /**
     * Create or append a descriptor with another stream.
     *
     * @param descriptor FlightDescriptor the descriptor for the data
     * @param metadataListener A handler for metadata messages from the server.
     * @param options RPC-layer hints for this call.
     * @return ClientStreamListener an interface to control uploading data.
     *     {@link ClientStreamListener#start(VectorSchemaRoot, DictionaryProvider)} will NOT already have been called.
     */
    public ClientStreamListener startPut(
            FlightDescriptor descriptor, PutListener metadataListener, CallOption... options) {
        Preconditions.checkNotNull(descriptor, "descriptor must not be null");
        Preconditions.checkNotNull(metadataListener, "metadataListener must not be null");

        try {
            ClientCall<ArrowMessage, Flight.PutResult> call = asyncStubNewCall(this.doPutDescriptor, options);
            OnStreamReadyHandler onStreamReadyHandler = new OnStreamReadyHandler();
            SetStreamObserver resultObserver =
                    new SetStreamObserver(this.allocator, metadataListener, onStreamReadyHandler);
            ClientCallStreamObserver<ArrowMessage> observer =
                    (ClientCallStreamObserver<ArrowMessage>) ClientCalls.asyncBidiStreamingCall(call, resultObserver);
            return new PutObserver(
                    descriptor,
                    observer,
                    metadataListener::isCancelled,
                    metadataListener::isCompletedExceptionally,
                    metadataListener::getResult,
                    onStreamReadyHandler,
                    this.compressionType);
        } catch (StatusRuntimeException sre) {
            throw StatusUtils.fromGrpcRuntimeException(sre);
        }
    }

    public DictionaryProvider newDefaultDictionaryProvider() {
        return new MapDictionaryProvider();
    }

    private static class OnStreamReadyHandler implements Runnable {
        private final Semaphore semaphore = new Semaphore(0);

        @Override
        public void run() {
            this.semaphore.release();
        }

        /**
         * Awaits for the stream to be ready for writing. Since the {@link #run()} method is not guaranteed
         * to be called in all circumstances, a timeout is always used to prevent indefinite blocking.
         *
         * @param timeout The timeout to wait for the stream to be ready.
         * @param unit The unit of the timeout.
         * @return True if the stream is ready, false if the timeout is reached.
         * @throws InterruptedException If the current thread is interrupted while waiting.
         */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return this.semaphore.tryAcquire(timeout, unit);
        }
    }

    /**
     * A stream observer for Flight.PutResult
     */
    private static class SetStreamObserver implements ClientResponseObserver<ArrowMessage, Flight.PutResult> {
        private final BufferAllocator allocator;
        private final StreamListener<PutResult> listener;
        private final OnStreamReadyHandler onStreamReadyHandler;

        SetStreamObserver(
                BufferAllocator allocator,
                StreamListener<PutResult> listener,
                OnStreamReadyHandler onStreamReadyHandler) {
            super();
            this.allocator = allocator;
            this.listener = listener == null ? NoOpStreamListener.getInstance() : listener;
            this.onStreamReadyHandler = onStreamReadyHandler;
        }

        @Override
        public void onNext(Flight.PutResult value) {
            try (PutResult message = PutResult.fromProtocol(this.allocator, value)) {
                this.listener.onNext(message);
            }
        }

        @Override
        public void onError(Throwable t) {
            this.listener.onError(StatusUtils.fromThrowable(t));
        }

        @Override
        public void onCompleted() {
            this.listener.onCompleted();
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<ArrowMessage> requestStream) {
            requestStream.setOnReadyHandler(this.onStreamReadyHandler);
        }
    }

    /**
     * The implementation of a {@link ClientStreamListener} for writing data to a Flight server.
     */
    static class PutObserver extends OutboundStreamListenerImpl implements ClientStreamListener {
        private final FlightDescriptor descriptor;
        private final BooleanSupplier isCancelled;
        private final BooleanSupplier isCompletedExceptionally;
        private final Runnable getResult;
        private final OnStreamReadyHandler onStreamReadyHandler;
        private final ArrowCompressionType compressionType;

        /**
         * Create a new client stream listener.
         *
         * @param descriptor The descriptor for the stream.
         * @param observer The write-side gRPC StreamObserver.
         * @param isCancelled A flag to check if the call has been cancelled.
         * @param isCompletedExceptionally A flag to check if the call has been completed exceptionally.
         * @param getResult A flag that blocks until the overall call completes.
         * @param compressionType The compression type to use.
         */
        PutObserver(
                FlightDescriptor descriptor,
                ClientCallStreamObserver<ArrowMessage> observer,
                BooleanSupplier isCancelled,
                BooleanSupplier isCompletedExceptionally,
                Runnable getResult,
                OnStreamReadyHandler onStreamReadyHandler,
                ArrowCompressionType compressionType) {
            super(descriptor, observer);
            Preconditions.checkNotNull(descriptor, "descriptor must be provided");
            Preconditions.checkNotNull(isCancelled, "isCancelled must be provided");
            Preconditions.checkNotNull(getResult, "getResult must be provided");
            this.descriptor = descriptor;
            this.isCancelled = isCancelled;
            this.isCompletedExceptionally = isCompletedExceptionally;
            this.getResult = getResult;
            this.onStreamReadyHandler = onStreamReadyHandler;
            this.compressionType = compressionType;
            this.unloader = null;
        }

        @Override
        public void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
            this.option = option;
            try {
                DictionaryUtils.generateSchemaMessages(
                        root.getSchema(), this.descriptor, dictionaries, option, super.responseObserver::onNext);
            } catch (RuntimeException e) {
                // Propagate runtime exceptions, like those raised when trying to write unions with V4 metadata
                throw e;
            } catch (Exception e) {
                // Only happens if closing buffers somehow fails - indicates application is an unknown state so
                // propagate
                // the exception
                throw new RuntimeException("Could not generate and send all schema messages", e);
            }

            CompressionCodec codec = null;
            switch (this.compressionType) {
                case Zstd:
                    codec = CommonsCompressionFactory.INSTANCE.createCodec(
                            org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD);
                    break;
                case Lz4:
                    // codec = CommonsCompressionFactory.INSTANCE.createCodec(
                    //         org.apache.arrow.vector.compression.CompressionUtil.CodecType.LZ4_FRAME);
                    throw new UnsupportedOperationException(
                            "LZ4 compression is not currently supported by the database");
                default:
                    break;
            }
            // We include the null count and align buffers to be compatible with Flight/C++
            super.unloader = new VectorUnloader(root, /* includeNullCount */ true, codec, /* alignBuffers */ true);
        }

        @Override
        protected void waitUntilStreamReady() {
            Timer.Context timerCtx = MetricsUtil.timer("bulk_flight_client.wait_until_stream_ready")
                    .time();
            try {
                // Check isCancelled as well to avoid inadvertently blocking forever
                // (so long as PutListener properly implements it)
                while (!super.responseObserver.isReady() && !this.isCancelled.getAsBoolean()) {
                    if (this.isCompletedExceptionally.getAsBoolean()) {
                        // Will throw the error immediately
                        getResult();
                    }

                    // If the stream is not ready, wait for a short time to avoid busy waiting
                    // This helps reduce CPU usage while still being responsive
                    try {
                        this.onStreamReadyHandler.await(10, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for stream to be ready", e);
                    }
                }
            } finally {
                timerCtx.stop();
            }
        }

        @Override
        public void getResult() {
            this.getResult.run();
        }
    }

    /**
     * Interface for writers to an Arrow data stream.
     */
    public interface ClientStreamListener extends OutboundStreamListener {

        /**
         * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
         * happened during the upload.
         */
        void getResult();
    }

    /**
     * A handler for server-sent application metadata messages during a Flight DoPut operation.
     *
     * <p>Generally, instead of implementing this yourself, you should use {@link AsyncPutListener} or {@link
     * SyncPutListener}.
     */
    public interface PutListener extends StreamListener<PutResult> {

        /**
         * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
         * happened during the upload.
         */
        void getResult();

        /**
         * Called when a message from the server is received.
         *
         * @param val The application metadata. This buffer will be reclaimed once onNext returns; you must retain a
         *     reference to use it outside this method.
         */
        @Override
        void onNext(PutResult val);

        /**
         * Check if the call has been cancelled.
         *
         * <p>By default, this always returns false. Implementations should provide an appropriate implementation, as
         * otherwise, a DoPut operation may inadvertently block forever.
         */
        default boolean isCancelled() {
            return false;
        }

        /**
         * Check if the call has been completed exceptionally.
         *
         * <p>By default, this always returns false. Implementations should provide an appropriate implementation, as
         * otherwise, a DoPut operation may inadvertently block forever.
         */
        default boolean isCompletedExceptionally() {
            return false;
        }
    }

    /**
     * Shut down this client.
     */
    public void close() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        allocator.close();
    }

    /**
     * Create a builder for a Flight client.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a builder for a Flight client.
     * @param allocator The allocator to use for the client.
     * @param location The location to connect to.
     */
    public static Builder builder(BufferAllocator allocator, Location location) {
        return new Builder(allocator, location);
    }

    /**
     * A builder for Flight clients.
     */
    public static final class Builder {
        private BufferAllocator allocator;
        private Location location;
        private int maxInboundMessageSize = FlightServer.MAX_GRPC_MESSAGE_SIZE;
        private List<FlightClientMiddleware.Factory> middleware = new ArrayList<>();
        private ArrowCompressionType compressionType = ArrowCompressionType.None;
        private TlsOptions tlsOptions;

        private Builder() {}

        private Builder(BufferAllocator allocator, Location location) {
            this.allocator = Preconditions.checkNotNull(allocator);
            this.location = Preconditions.checkNotNull(location);
        }

        /** Set the maximum inbound message size. */
        public Builder maxInboundMessageSize(int maxSize) {
            Preconditions.checkArgument(maxSize > 0);
            this.maxInboundMessageSize = maxSize;
            return this;
        }

        public Builder allocator(BufferAllocator allocator) {
            this.allocator = Preconditions.checkNotNull(allocator);
            return this;
        }

        public Builder location(Location location) {
            this.location = Preconditions.checkNotNull(location);
            return this;
        }

        public Builder intercept(FlightClientMiddleware.Factory factory) {
            middleware.add(factory);
            return this;
        }

        public Builder compressionType(ArrowCompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        public Builder tlsOptions(TlsOptions tlsOptions) {
            this.tlsOptions = tlsOptions;
            return this;
        }

        /**
         * Create the client from this builder.
         */
        public BulkFlightClient build() {
            NettyChannelBuilder builder;

            switch (this.location.getUri().getScheme()) {
                case LocationSchemes.GRPC:
                case LocationSchemes.GRPC_INSECURE:
                case LocationSchemes.GRPC_TLS: {
                    builder = NettyChannelBuilder.forAddress(this.location.toSocketAddress());
                    break;
                }
                default:
                    throw new IllegalArgumentException(
                            "Scheme is not supported: " + this.location.getUri().getScheme());
            }

            if (this.tlsOptions != null) {
                builder.useTransportSecurity();
                try {
                    SslContextBuilder sslContextBuilder = GrpcSslContexts.forClient();
                    Optional<File> clientCertChain = this.tlsOptions.getClientCertChain();
                    Optional<File> privateKey = this.tlsOptions.getPrivateKey();
                    Optional<String> privateKeyPassword = this.tlsOptions.getPrivateKeyPassword();

                    if (clientCertChain.isPresent() && privateKey.isPresent()) {
                        if (privateKeyPassword.isPresent()) {
                            sslContextBuilder.keyManager(
                                    clientCertChain.get(), privateKey.get(), privateKeyPassword.get());
                        } else {
                            sslContextBuilder.keyManager(clientCertChain.get(), privateKey.get());
                        }
                    }

                    this.tlsOptions.getRootCerts().ifPresent(sslContextBuilder::trustManager);
                    builder.sslContext(sslContextBuilder.build());
                } catch (SSLException e) {
                    throw new RuntimeException("Failed to configure SslContext", e);
                }
            } else {
                builder.usePlaintext();
            }

            builder.maxTraceEvents(MAX_CHANNEL_TRACE_EVENTS).maxInboundMessageSize(this.maxInboundMessageSize);
            return new BulkFlightClient(this.allocator, builder.build(), this.middleware, this.compressionType);
        }
    }

    /**
     * Helper method to create a call from the asyncStub, method descriptor, and list of calling options.
     */
    private <RequestT, ResponseT> ClientCall<RequestT, ResponseT> asyncStubNewCall(
            MethodDescriptor<RequestT, ResponseT> descriptor, CallOption... options) {
        FlightServiceStub wrappedStub = CallOptions.wrapStub(asyncStub, options);
        return wrappedStub.getChannel().newCall(descriptor, wrappedStub.getCallOptions());
    }
}
