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
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLException;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth.ClientAuthInterceptor;
import org.apache.arrow.flight.auth.ClientAuthWrapper;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientHandshakeWrapper;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.ClientInterceptorAdapter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.grpc.StatusUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.flight.impl.FlightServiceGrpc.FlightServiceStub;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;

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
    private final ClientAuthInterceptor authInterceptor = new ClientAuthInterceptor();
    private final MethodDescriptor<ArrowMessage, Flight.PutResult> doPutDescriptor;
    private final List<FlightClientMiddleware.Factory> middleware;

    /**
     * Create a Flight client from an allocator and a gRPC channel.
     */
    BulkFlightClient(
            BufferAllocator incomingAllocator,
            ManagedChannel channel,
            List<FlightClientMiddleware.Factory> middleware) {
        this.allocator = incomingAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE);
        this.channel = channel;
        this.middleware = middleware;

        ClientInterceptor[] interceptors =
                new ClientInterceptor[] {authInterceptor, new ClientInterceptorAdapter(middleware)};

        // Create a channel with interceptors pre-applied for DoGet and DoPut
        Channel interceptedChannel = ClientInterceptors.intercept(channel, interceptors);

        this.asyncStub = FlightServiceGrpc.newStub(interceptedChannel);
        this.doPutDescriptor = FlightBindingService.getDoPutDescriptor(this.allocator);
    }

    /**
     * Authenticates with a username and password.
     */
    public void authenticateBasic(String username, String password) {
        BasicClientAuthHandler basicClient = new BasicClientAuthHandler(username, password);
        authenticate(basicClient);
    }

    /**
     * Authenticates against the Flight service.
     *
     * @param options RPC-layer hints for this call.
     * @param handler The auth mechanism to use.
     */
    public void authenticate(
            @SuppressWarnings("deprecation") org.apache.arrow.flight.auth.ClientAuthHandler handler,
            CallOption... options) {
        Preconditions.checkArgument(!this.authInterceptor.hasAuthHandler(), "Auth already completed.");
        ClientAuthWrapper.doClientAuth(handler, CallOptions.wrapStub(this.asyncStub, options));
        this.authInterceptor.setAuthHandler(handler);
    }

    /**
     * Authenticates with a username and password.
     *
     * @param username the username.
     * @param password the password.
     * @return a CredentialCallOption containing a bearer token if the server emitted one, or
     *     empty if no bearer token was returned. This can be used in subsequent API calls.
     */
    public Optional<CredentialCallOption> authenticateBasicToken(String username, String password) {
        final ClientIncomingAuthHeaderMiddleware.Factory clientAuthMiddleware =
                new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
        this.middleware.add(clientAuthMiddleware);
        handshake(new CredentialCallOption(new BasicAuthCredentialWriter(username, password)));

        return Optional.ofNullable(clientAuthMiddleware.getCredentialCallOption());
    }

    /**
     * Executes the handshake against the Flight service.
     *
     * @param options RPC-layer hints for this call.
     */
    public void handshake(CallOption... options) {
        ClientHandshakeWrapper.doClientHandshake(CallOptions.wrapStub(this.asyncStub, options));
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
            FlightDescriptor descriptor,
            VectorSchemaRoot root,
            PutListener metadataListener,
            Runnable onReadyHandler,
            CallOption... options) {
        return startPut(descriptor, root, new MapDictionaryProvider(), metadataListener, onReadyHandler, options);
    }

    /**
     * Create or append a descriptor with another stream.
     * @param descriptor FlightDescriptor the descriptor for the data
     * @param root VectorSchemaRoot the root containing data
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
            Runnable onReadyHandler,
            CallOption... options) {
        Preconditions.checkNotNull(root, "root must not be null");
        Preconditions.checkNotNull(provider, "provider must not be null");
        ClientStreamListener writer = startPut(descriptor, metadataListener, onReadyHandler, options);
        writer.start(root, provider);
        return writer;
    }

    /**
     * Create or append a descriptor with another stream.
     * @param descriptor FlightDescriptor the descriptor for the data
     * @param metadataListener A handler for metadata messages from the server.
     * @param options RPC-layer hints for this call.
     * @return ClientStreamListener an interface to control uploading data.
     *     {@link ClientStreamListener#start(VectorSchemaRoot, DictionaryProvider)} will NOT already have been called.
     */
    public ClientStreamListener startPut(
            FlightDescriptor descriptor, PutListener metadataListener, Runnable onReadyHandler, CallOption... options) {
        Preconditions.checkNotNull(descriptor, "descriptor must not be null");
        Preconditions.checkNotNull(metadataListener, "metadataListener must not be null");

        try {
            ClientCall<ArrowMessage, Flight.PutResult> call = asyncStubNewCall(this.doPutDescriptor, options);
            SetStreamObserver resultObserver = new SetStreamObserver(this.allocator, metadataListener, onReadyHandler);
            ClientCallStreamObserver<ArrowMessage> observer =
                    (ClientCallStreamObserver<ArrowMessage>) ClientCalls.asyncBidiStreamingCall(call, resultObserver);
            return new PutObserver(descriptor, observer, metadataListener::isCancelled, metadataListener::getResult);
        } catch (StatusRuntimeException sre) {
            throw StatusUtils.fromGrpcRuntimeException(sre);
        }
    }

    public DictionaryProvider newDefaultDictionaryProvider() {
        return new MapDictionaryProvider();
    }

    /**
     * A stream observer for Flight.PutResult
     */
    private static class SetStreamObserver implements ClientResponseObserver<ArrowMessage, Flight.PutResult> {
        private final BufferAllocator allocator;
        private final StreamListener<PutResult> listener;
        private final Runnable onReadyHandler;

        SetStreamObserver(BufferAllocator allocator, StreamListener<PutResult> listener, Runnable onReadyHandler) {
            super();
            this.allocator = allocator;
            this.listener = listener == null ? NoOpStreamListener.getInstance() : listener;
            this.onReadyHandler = onReadyHandler;
        }

        @Override
        public void onNext(Flight.PutResult value) {
            try (final PutResult message = PutResult.fromProtocol(this.allocator, value)) {
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
            requestStream.setOnReadyHandler(this.onReadyHandler);
        }
    }

    /**
     * The implementation of a {@link ClientStreamListener} for writing data to a Flight server.
     */
    static class PutObserver extends OutboundStreamListenerImpl implements ClientStreamListener {
        private final BooleanSupplier isCancelled;
        private final Runnable getResult;

        /**
         * Create a new client stream listener.
         *
         * @param descriptor The descriptor for the stream.
         * @param observer The write-side gRPC StreamObserver.
         * @param isCancelled A flag to check if the call has been cancelled.
         * @param getResult A flag that blocks until the overall call completes.
         */
        PutObserver(
                FlightDescriptor descriptor,
                ClientCallStreamObserver<ArrowMessage> observer,
                BooleanSupplier isCancelled,
                Runnable getResult) {
            super(descriptor, observer);
            Preconditions.checkNotNull(descriptor, "descriptor must be provided");
            Preconditions.checkNotNull(isCancelled, "isCancelled must be provided");
            Preconditions.checkNotNull(getResult, "getResult must be provided");
            this.isCancelled = isCancelled;
            this.getResult = getResult;
            this.unloader = null;
        }

        @Override
        protected void waitUntilStreamReady() {
            // Check isCancelled as well to avoid inadvertently blocking forever
            // (so long as PutListener properly implements it)
            while (!super.responseObserver.isReady() && !this.isCancelled.getAsBoolean()) {
                /* busy wait */
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
            return new BulkFlightClient(this.allocator, builder.build(), this.middleware);
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
