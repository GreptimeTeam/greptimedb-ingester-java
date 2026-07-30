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

import io.greptime.ArrowCompressionType;
import io.greptime.rpc.RpcOptions;
import io.greptime.rpc.TlsOptions;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BulkFlightClientTest {

    @Test
    public void testRpcOptionsConfigureChannel() throws Exception {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setMaxInboundMessageSize(1_234_567);
        rpcOptions.setFlowControlWindow(2_345_678);
        rpcOptions.setIdleTimeoutSeconds(47);
        rpcOptions.setKeepAliveTimeSeconds(53);
        rpcOptions.setKeepAliveTimeoutSeconds(59);
        rpcOptions.setKeepAliveWithoutCalls(true);

        BulkFlightClient.Builder clientBuilder = BulkFlightClient.builder().rpcOptions(rpcOptions);
        rpcOptions.setMaxInboundMessageSize(1);
        rpcOptions.setFlowControlWindow(1);
        rpcOptions.setIdleTimeoutSeconds(1);
        rpcOptions.setKeepAliveTimeSeconds(1);
        rpcOptions.setKeepAliveTimeoutSeconds(1);
        rpcOptions.setKeepAliveWithoutCalls(false);

        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress("localhost", 4001);
        clientBuilder.configureChannel(channelBuilder);
        NettyChannelBuilderInspector inspector = NettyChannelBuilderInspector.inspect(channelBuilder);

        Assert.assertEquals(1_234_567, inspector.maxInboundMessageSize());
        Assert.assertEquals(2_345_678, inspector.flowControlWindow());
        Assert.assertEquals(47, inspector.idleTimeoutSeconds());
        Assert.assertEquals(53, inspector.keepAliveTimeSeconds());
        Assert.assertEquals(59, inspector.keepAliveTimeoutSeconds());
        Assert.assertTrue(inspector.keepAliveWithoutCalls());
        Assert.assertEquals(0, inspector.maxTraceEvents());
    }

    @Test
    public void testExplicitMaxInboundMessageSizeOverridesRpcOptions() throws Exception {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setMaxInboundMessageSize(1_234_567);
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress("localhost", 4001);

        BulkFlightClient.builder()
                .rpcOptions(rpcOptions)
                .maxInboundMessageSize(7_654_321)
                .configureChannel(channelBuilder);

        Assert.assertEquals(
                7_654_321, NettyChannelBuilderInspector.inspect(channelBuilder).maxInboundMessageSize());
    }

    @Test
    public void testRpcOptionsDefaultWhenAbsent() throws Exception {
        RpcOptions defaults = RpcOptions.newDefault();
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress("localhost", 4001);

        BulkFlightClient.builder().configureChannel(channelBuilder);
        NettyChannelBuilderInspector inspector = NettyChannelBuilderInspector.inspect(channelBuilder);

        Assert.assertEquals(defaults.getMaxInboundMessageSize(), inspector.maxInboundMessageSize());
        Assert.assertEquals(defaults.getFlowControlWindow(), inspector.flowControlWindow());
        Assert.assertEquals(defaults.getKeepAliveTimeSeconds(), inspector.keepAliveTimeSeconds());
    }

    @Test
    public void testTlsResolutionPrefersExplicitThenRpcThenPlaintext() throws Exception {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        TlsOptions invalidRpcTls = new TlsOptions();
        invalidRpcTls.setRootCerts(new File("missing-rpc-root-certificate.pem"));
        rpcOptions.setTlsOptions(invalidRpcTls);

        NettyChannelBuilder explicitTlsChannel = NettyChannelBuilder.forAddress("localhost", 4001);
        BulkFlightClient.builder()
                .rpcOptions(rpcOptions)
                .tlsOptions(new TlsOptions())
                .configureChannel(explicitTlsChannel);

        RpcOptions rpcTlsOptions = RpcOptions.newDefault();
        rpcTlsOptions.setTlsOptions(new TlsOptions());
        NettyChannelBuilder rpcTlsChannel = NettyChannelBuilder.forAddress("localhost", 4001);
        BulkFlightClient.builder().rpcOptions(rpcTlsOptions).configureChannel(rpcTlsChannel);

        NettyChannelBuilder plaintextChannel = NettyChannelBuilder.forAddress("localhost", 4001);
        BulkFlightClient.builder().configureChannel(plaintextChannel);

        Assert.assertEquals(
                NegotiationType.TLS,
                NettyChannelBuilderInspector.inspect(explicitTlsChannel).negotiationType());
        Assert.assertEquals(
                NegotiationType.TLS,
                NettyChannelBuilderInspector.inspect(rpcTlsChannel).negotiationType());
        Assert.assertEquals(
                NegotiationType.PLAINTEXT,
                NettyChannelBuilderInspector.inspect(plaintextChannel).negotiationType());
    }

    @Test
    public void testConstructorFailureClosesChildAllocator() throws Exception {
        try (BufferAllocator parentAllocator = new RootAllocator(Long.MAX_VALUE)) {
            RuntimeException failure = null;
            int childrenAfter;
            try {
                new BulkFlightClient(parentAllocator, null, Collections.emptyList(), ArrowCompressionType.None);
                Assert.fail("Expected null channel to fail client construction");
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                childrenAfter = parentAllocator.getChildAllocators().size();
                for (BufferAllocator child : new ArrayList<>(parentAllocator.getChildAllocators())) {
                    child.close();
                }
            }

            Assert.assertTrue(failure instanceof NullPointerException);
            Assert.assertEquals(0, childrenAfter);
        }
    }

    @Test
    public void testBuilderClosesChannelWhenClientConstructionFails() {
        BufferAllocator allocator = Mockito.mock(BufferAllocator.class);
        RuntimeException constructionFailure = new RuntimeException("child allocation failed");
        Mockito.when(allocator.newChildAllocator("bulk-flight-client", 0, Long.MAX_VALUE))
                .thenThrow(constructionFailure);
        ManagedChannel channel = Mockito.mock(ManagedChannel.class);
        Mockito.when(channel.shutdownNow()).thenReturn(channel);

        RuntimeException failure = null;
        try {
            BulkFlightClient.builder().allocator(allocator).build(channel);
            Assert.fail("Expected client construction to fail");
        } catch (RuntimeException e) {
            failure = e;
        }

        Assert.assertSame(constructionFailure, failure);
        Mockito.verify(channel).shutdownNow();
    }

    @Test
    public void testReadinessTimeoutCancelsFlightCall() {
        @SuppressWarnings("unchecked")
        ClientCallStreamObserver<ArrowMessage> observer = Mockito.mock(ClientCallStreamObserver.class);
        Mockito.when(observer.isReady()).thenReturn(false);
        BulkFlightClient.PutObserver putObserver = new BulkFlightClient.PutObserver(
                FlightDescriptor.path("metrics"),
                observer,
                () -> false,
                () -> false,
                () -> {},
                new BulkFlightClient.OnStreamReadyHandler(0),
                10L,
                ArrowCompressionType.None);

        FlightRuntimeException failure = null;
        try {
            putObserver.waitUntilStreamReady();
            Assert.fail("Expected readiness timeout");
        } catch (FlightRuntimeException e) {
            failure = e;
        }

        Assert.assertEquals(FlightStatusCode.TIMED_OUT, failure.status().code());
        Mockito.verify(observer).cancel(Mockito.eq("Bulk write stream readiness timed out"), Mockito.same(failure));
    }

    @Test
    public void testReadinessPermitAllowsPipelinedWrite() {
        @SuppressWarnings("unchecked")
        ClientCallStreamObserver<ArrowMessage> observer = Mockito.mock(ClientCallStreamObserver.class);
        Mockito.when(observer.isReady()).thenReturn(false);
        BulkFlightClient.PutObserver putObserver = new BulkFlightClient.PutObserver(
                FlightDescriptor.path("metrics"),
                observer,
                () -> false,
                () -> false,
                () -> {},
                new BulkFlightClient.OnStreamReadyHandler(1),
                10L,
                ArrowCompressionType.None);

        putObserver.waitUntilStreamReady();

        Mockito.verify(observer, Mockito.never()).cancel(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCloseForcesChannelShutdownAfterGracePeriod() throws Exception {
        BufferAllocator parentAllocator = Mockito.mock(BufferAllocator.class);
        BufferAllocator childAllocator = Mockito.mock(BufferAllocator.class);
        Mockito.when(parentAllocator.newChildAllocator("bulk-flight-client", 0, Long.MAX_VALUE))
                .thenReturn(childAllocator);
        ManagedChannel channel = Mockito.mock(ManagedChannel.class);
        Mockito.when(channel.shutdown()).thenReturn(channel);
        Mockito.when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(false);
        Mockito.when(channel.shutdownNow()).thenReturn(channel);
        BulkFlightClient client =
                new BulkFlightClient(parentAllocator, channel, Collections.emptyList(), ArrowCompressionType.None);

        client.close();

        Mockito.verify(channel).shutdownNow();
        Mockito.verify(channel).awaitTermination(1, TimeUnit.SECONDS);
        Mockito.verify(childAllocator).close();
    }

    @Test
    public void testCloseClosesChildAllocatorWhenChannelShutdownIsInterrupted() throws Exception {
        Thread.interrupted();
        try {
            BufferAllocator parentAllocator = Mockito.mock(BufferAllocator.class);
            BufferAllocator childAllocator = Mockito.mock(BufferAllocator.class);
            Mockito.when(parentAllocator.newChildAllocator("bulk-flight-client", 0, Long.MAX_VALUE))
                    .thenReturn(childAllocator);
            ManagedChannel channel = Mockito.mock(ManagedChannel.class);
            Mockito.when(channel.shutdown()).thenReturn(channel);
            InterruptedException interruption = new InterruptedException("channel shutdown interrupted");
            Mockito.when(channel.awaitTermination(5, TimeUnit.SECONDS)).thenThrow(interruption);
            BulkFlightClient client =
                    new BulkFlightClient(parentAllocator, channel, Collections.emptyList(), ArrowCompressionType.None);

            InterruptedException failure = null;
            try {
                client.close();
                Assert.fail("Expected interrupted channel shutdown");
            } catch (InterruptedException e) {
                failure = e;
            }

            Assert.assertSame(interruption, failure);
            Mockito.verify(channel).shutdownNow();
            Mockito.verify(channel).awaitTermination(1, TimeUnit.SECONDS);
            Mockito.verify(childAllocator).close();
            Assert.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}
