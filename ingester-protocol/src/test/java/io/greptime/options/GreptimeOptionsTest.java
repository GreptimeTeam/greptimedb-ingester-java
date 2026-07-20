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

package io.greptime.options;

import io.greptime.Router;
import io.greptime.common.Endpoint;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import io.greptime.rpc.RpcOptions;
import io.greptime.rpc.TlsOptions;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GreptimeOptionsTest {

    @Test
    public void testAllOptions() {
        String database = "greptime.public";
        String[] endpoints = {"127.0.0.1:4001"};
        Executor asyncPool = command -> System.out.println("asyncPool");
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setMaxInboundMessageSize(1024);
        rpcOptions.setFlowControlWindow(2048);
        rpcOptions.setIdleTimeoutSeconds(30);
        rpcOptions.setKeepAliveTimeSeconds(40);
        rpcOptions.setKeepAliveTimeoutSeconds(5);
        rpcOptions.setKeepAliveWithoutCalls(true);
        int writeMaxRetries = 2;
        int maxInFlightWritePoints = 9990;
        LimitedPolicy limitedPolicy = new LimitedPolicy.DiscardPolicy();
        int defaultStreamMaxWritePointsPerSecond = 100000;
        long routeTableRefreshPeriodSeconds = 99;
        long checkHealthTimeoutMs = 1000;
        AuthInfo authInfo = new AuthInfo("user", "password");
        Router<Void, Endpoint> router = createTestRouter();
        TlsOptions tlsOptions = new TlsOptions();

        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
                .asyncPool(asyncPool)
                .rpcOptions(rpcOptions)
                .tlsOptions(tlsOptions)
                .writeMaxRetries(writeMaxRetries)
                .maxInFlightWritePoints(maxInFlightWritePoints)
                .writeLimitedPolicy(limitedPolicy)
                .defaultStreamMaxWritePointsPerSecond(defaultStreamMaxWritePointsPerSecond)
                .routeTableRefreshPeriodSeconds(routeTableRefreshPeriodSeconds)
                .checkHealthTimeoutMs(checkHealthTimeoutMs)
                .authInfo(authInfo)
                .router(router)
                .build();

        Assert.assertEquals(database, opts.getDatabase());
        Assert.assertArrayEquals(
                endpoints, opts.getEndpoints().stream().map(Endpoint::toString).toArray());
        Assert.assertNotSame(rpcOptions, opts.getRpcOptions());
        assertChannelOptions(rpcOptions, opts.getRpcOptions());
        Assert.assertNull(rpcOptions.getTlsOptions());
        Assert.assertSame(tlsOptions, opts.getRpcOptions().getTlsOptions());

        RouterOptions routerOptions = opts.getRouterOptions();
        Assert.assertNotNull(routerOptions);
        Assert.assertArrayEquals(
                endpoints,
                routerOptions.getEndpoints().stream().map(Endpoint::toString).toArray());
        Assert.assertEquals(router, routerOptions.getRouter());
        Assert.assertEquals(routeTableRefreshPeriodSeconds, routerOptions.getRefreshPeriodSeconds());
        Assert.assertEquals(checkHealthTimeoutMs, routerOptions.getCheckHealthTimeoutMs());

        WriteOptions writeOptions = opts.getWriteOptions();
        Assert.assertNotNull(writeOptions);
        Assert.assertEquals(asyncPool, writeOptions.getAsyncPool());
        Assert.assertEquals(writeMaxRetries, writeOptions.getMaxRetries());
        Assert.assertEquals(maxInFlightWritePoints, writeOptions.getMaxInFlightWritePoints());
        Assert.assertEquals(limitedPolicy, writeOptions.getLimitedPolicy());
        Assert.assertEquals(
                defaultStreamMaxWritePointsPerSecond, writeOptions.getDefaultStreamMaxWritePointsPerSecond());
        Assert.assertEquals(authInfo, writeOptions.getAuthInfo());

        BulkWriteOptions bulkWriteOptions = opts.getBulkWriteOptions();
        Assert.assertNotSame(rpcOptions, bulkWriteOptions.getRpcOptions());
        assertChannelOptions(rpcOptions, bulkWriteOptions.getRpcOptions());

        BulkWriteOptions copiedBulkWriteOptions = bulkWriteOptions.copy();
        Assert.assertNotSame(bulkWriteOptions.getRpcOptions(), copiedBulkWriteOptions.getRpcOptions());
        assertChannelOptions(bulkWriteOptions.getRpcOptions(), copiedBulkWriteOptions.getRpcOptions());
    }

    @Test
    public void testBulkWriteOptionsCopyWithoutRpcOptions() {
        BulkWriteOptions copied = new BulkWriteOptions().copy();

        Assert.assertNull(copied.getRpcOptions());
    }

    @Test
    public void testBulkWriteOptionsCopyHasIndependentTlsOptions() {
        TlsOptions tlsOptions = tlsOptions("original.pem");
        BulkWriteOptions original = new BulkWriteOptions();
        original.setTlsOptions(tlsOptions);

        BulkWriteOptions copied = original.copy();

        Assert.assertNotSame(tlsOptions, copied.getTlsOptions());
        Assert.assertEquals(
                new File("original.pem"), copied.getTlsOptions().getRootCerts().get());

        copied.getTlsOptions().setRootCerts(new File("copied.pem"));
        Assert.assertEquals(
                new File("original.pem"),
                original.getTlsOptions().getRootCerts().get());
    }

    @Test
    public void testCopyPreservesIndependentBulkWriteOptions() {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setMaxInboundMessageSize(1024);
        GreptimeOptions original = GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public")
                .rpcOptions(rpcOptions)
                .useZeroCopyWriteInBulkWrite(true)
                .build();

        GreptimeOptions copied = original.copy();

        Assert.assertSame(copied, GreptimeOptions.checkSelf(copied));
        Assert.assertNotSame(original.getBulkWriteOptions(), copied.getBulkWriteOptions());
        Assert.assertNotSame(
                original.getBulkWriteOptions().getRpcOptions(),
                copied.getBulkWriteOptions().getRpcOptions());
        Assert.assertEquals(
                original.getBulkWriteOptions().isUseZeroCopyWrite(),
                copied.getBulkWriteOptions().isUseZeroCopyWrite());
        Assert.assertEquals(
                original.getBulkWriteOptions().getRpcOptions().getMaxInboundMessageSize(),
                copied.getBulkWriteOptions().getRpcOptions().getMaxInboundMessageSize());

        copied.getBulkWriteOptions().setUseZeroCopyWrite(false);
        copied.getBulkWriteOptions().getRpcOptions().setMaxInboundMessageSize(2048);

        Assert.assertTrue(original.getBulkWriteOptions().isUseZeroCopyWrite());
        Assert.assertEquals(1024, original.getBulkWriteOptions().getRpcOptions().getMaxInboundMessageSize());
    }

    @Test
    public void testExplicitNullTlsOverridesRpcTlsRegardlessOfSetterOrder() {
        RpcOptions rpcOptionsSetLast = rpcOptionsWithTls();
        GreptimeOptions tlsSetFirst = GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public")
                .tlsOptions(null)
                .rpcOptions(rpcOptionsSetLast)
                .build();

        RpcOptions rpcOptionsSetFirst = rpcOptionsWithTls();
        GreptimeOptions tlsSetLast = GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public")
                .rpcOptions(rpcOptionsSetFirst)
                .tlsOptions(null)
                .build();

        assertTlsCleared(tlsSetFirst);
        assertTlsCleared(tlsSetLast);
    }

    @Test
    public void testRpcTlsPreservedWhenTlsOptionsNotConfigured() {
        RpcOptions rpcOptions = rpcOptionsWithTls();

        GreptimeOptions opts = GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public")
                .rpcOptions(rpcOptions)
                .build();

        Assert.assertSame(rpcOptions, opts.getRpcOptions());
        Assert.assertSame(rpcOptions.getTlsOptions(), opts.getRpcOptions().getTlsOptions());
        Assert.assertNotSame(
                rpcOptions.getTlsOptions(),
                opts.getBulkWriteOptions().getRpcOptions().getTlsOptions());
        Assert.assertEquals(
                rpcOptions.getTlsOptions().getRootCerts(),
                opts.getBulkWriteOptions().getRpcOptions().getTlsOptions().getRootCerts());
    }

    @Test
    public void testExplicitTlsOverridesWithoutMutatingInputOrPreviousOptions() {
        RpcOptions rpcOptions = rpcOptionsWithTls();
        TlsOptions originalTlsOptions = rpcOptions.getTlsOptions();
        GreptimeOptions.Builder builder =
                GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public").rpcOptions(rpcOptions);
        GreptimeOptions previous = builder.build();
        TlsOptions explicitTlsOptions = tlsOptions("explicit.pem");

        GreptimeOptions opts = builder.tlsOptions(explicitTlsOptions).build();

        Assert.assertSame(rpcOptions, previous.getRpcOptions());
        Assert.assertSame(originalTlsOptions, rpcOptions.getTlsOptions());
        Assert.assertSame(originalTlsOptions, previous.getRpcOptions().getTlsOptions());
        Assert.assertNotSame(rpcOptions, opts.getRpcOptions());
        Assert.assertSame(explicitTlsOptions, opts.getRpcOptions().getTlsOptions());
        Assert.assertNotSame(
                explicitTlsOptions, opts.getBulkWriteOptions().getRpcOptions().getTlsOptions());
        Assert.assertEquals(
                explicitTlsOptions.getRootCerts(),
                opts.getBulkWriteOptions().getRpcOptions().getTlsOptions().getRootCerts());
        Assert.assertSame(explicitTlsOptions, opts.getBulkWriteOptions().getTlsOptions());
    }

    @Test
    public void testExplicitNullTlsDoesNotMutateInputOrPreviousOptions() {
        RpcOptions rpcOptions = rpcOptionsWithTls();
        TlsOptions originalTlsOptions = rpcOptions.getTlsOptions();
        GreptimeOptions.Builder builder =
                GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public").rpcOptions(rpcOptions);
        GreptimeOptions previous = builder.build();

        GreptimeOptions plaintext = builder.tlsOptions(null).build();

        Assert.assertSame(rpcOptions, previous.getRpcOptions());
        Assert.assertSame(originalTlsOptions, rpcOptions.getTlsOptions());
        Assert.assertSame(originalTlsOptions, previous.getRpcOptions().getTlsOptions());
        Assert.assertNotSame(rpcOptions, plaintext.getRpcOptions());
        assertTlsCleared(plaintext);
    }

    @Test
    public void testExplicitNullTlsRemainsEffectiveWhenBuilderReused() {
        GreptimeOptions.Builder builder = GreptimeOptions.newBuilder("127.0.0.1:4001", "greptime.public")
                .rpcOptions(rpcOptionsWithTls())
                .tlsOptions(null);

        assertTlsCleared(builder.build());

        builder.rpcOptions(rpcOptionsWithTls());
        assertTlsCleared(builder.build());
    }

    private void assertChannelOptions(RpcOptions expected, RpcOptions actual) {
        Assert.assertEquals(expected.getMaxInboundMessageSize(), actual.getMaxInboundMessageSize());
        Assert.assertEquals(expected.getFlowControlWindow(), actual.getFlowControlWindow());
        Assert.assertEquals(expected.getIdleTimeoutSeconds(), actual.getIdleTimeoutSeconds());
        Assert.assertEquals(expected.getKeepAliveTimeSeconds(), actual.getKeepAliveTimeSeconds());
        Assert.assertEquals(expected.getKeepAliveTimeoutSeconds(), actual.getKeepAliveTimeoutSeconds());
        Assert.assertEquals(expected.isKeepAliveWithoutCalls(), actual.isKeepAliveWithoutCalls());
    }

    private RpcOptions rpcOptionsWithTls() {
        RpcOptions rpcOptions = RpcOptions.newDefault();
        rpcOptions.setTlsOptions(tlsOptions("rpc.pem"));
        return rpcOptions;
    }

    private TlsOptions tlsOptions(String rootCerts) {
        TlsOptions tlsOptions = new TlsOptions();
        tlsOptions.setRootCerts(new File(rootCerts));
        return tlsOptions;
    }

    private void assertTlsCleared(GreptimeOptions opts) {
        Assert.assertNull(opts.getRpcOptions().getTlsOptions());
        Assert.assertNull(opts.getBulkWriteOptions().getRpcOptions().getTlsOptions());
        Assert.assertNull(opts.getBulkWriteOptions().getTlsOptions());
    }

    private Router<Void, Endpoint> createTestRouter() {
        return new Router<Void, Endpoint>() {
            @Override
            public CompletableFuture<Endpoint> routeFor(Void request) {
                return null;
            }

            @Override
            public void onRefresh(List<Endpoint> activities, List<Endpoint> inactivities) {}
        };
    }
}
