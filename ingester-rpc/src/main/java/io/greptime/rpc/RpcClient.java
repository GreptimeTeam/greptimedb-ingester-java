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

package io.greptime.rpc;

import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Lifecycle;

/**
 * A common RPC client interface.
 */
public interface RpcClient extends Lifecycle<RpcOptions>, Display {

    /**
     * Closes all connections of an address.
     *
     * @param endpoint target address
     */
    @SuppressWarnings("unused")
    void closeConnection(Endpoint endpoint);

    /**
     * Register a connection state observer.
     *
     * @param observer connection state observer
     */
    void registerConnectionObserver(ConnectionObserver observer);

    /**
     * A connection observer.
     */
    interface ConnectionObserver {

        /**
         * The channel has successfully established a connection.
         *
         * @param ep the server endpoint
         */
        void onReady(Endpoint ep);

        /**
         * There has been some transient failure (such as a TCP 3-way handshake timing
         * out or a socket error).
         *
         * @param ep the server endpoint
         */
        void onFailure(Endpoint ep);

        /**
         * This channel has started shutting down. Any new RPCs should fail immediately.
         *
         * @param ep the server endpoint
         */
        void onShutdown(Endpoint ep);
    }

    /**
     * Executes an asynchronous call with a response {@link Observer}.
     *
     * @param endpoint the target address
     * @param request the request object
     * @param observer the response observer
     * @param timeoutMs timeout with millisecond
     * @param <Req> the request message type
     * @param <Resp> the response message type
     */
    @SuppressWarnings("unused")
    default <Req, Resp> void invokeAsync(Endpoint endpoint, Req request, Observer<Resp> observer, long timeoutMs) {
        invokeAsync(endpoint, request, null, observer, timeoutMs);
    }

    /**
     * Executes an asynchronous call with a response {@link Observer}.
     *
     * @param endpoint the target address
     * @param request the request object
     * @param ctx the invoke context
     * @param observer the response observer
     * @param timeoutMs timeout with millisecond
     * @param <Req> the request message type
     * @param <Resp> the response message type
     */
    <Req, Resp> void invokeAsync(Endpoint endpoint, Req request, Context ctx, Observer<Resp> observer, long timeoutMs);

    /**
     * Executes a server-streaming call with a response {@link Observer}.
     * <p>
     * One request message followed by zero or more response messages.
     *
     * @param endpoint the target address
     * @param request the request object
     * @param ctx the invoke context
     * @param observer the response stream observer
     * @param <Req> the request message type
     * @param <Resp> the response message type
     */
    <Req, Resp> void invokeServerStreaming(Endpoint endpoint, Req request, Context ctx, Observer<Resp> observer);

    /**
     * Executes a client-streaming call with a request {@link Observer}
     * and a response {@link Observer}.
     *
     * @param endpoint the target address
     * @param defaultReqIns the default request instance
     * @param ctx the invoke context
     * @param respObserver the response stream observer
     * @param <Req> the request message type
     * @param <Resp> the response message type
     * @return request {@link Observer}.
     */
    <Req, Resp> Observer<Req> invokeClientStreaming(
            Endpoint endpoint, Req defaultReqIns, Context ctx, Observer<Resp> respObserver);
}
