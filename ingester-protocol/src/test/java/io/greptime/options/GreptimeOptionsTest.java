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
        int writeMaxRetries = 2;
        int maxInFlightWritePoints = 9990;
        LimitedPolicy limitedPolicy = new LimitedPolicy.DiscardPolicy();
        int defaultStreamMaxWritePointsPerSecond = 100000;
        long routeTableRefreshPeriodSeconds = 99;
        AuthInfo authInfo = new AuthInfo("user", "password");
        Router<Void, Endpoint> router = createTestRouter();
        TlsOptions tlsOptions = new TlsOptions();

        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database) //
                .asyncPool(asyncPool) //
                .rpcOptions(rpcOptions) //
                .tlsOptions(tlsOptions) //
                .writeMaxRetries(writeMaxRetries) //
                .maxInFlightWritePoints(maxInFlightWritePoints) //
                .writeLimitedPolicy(limitedPolicy) //
                .defaultStreamMaxWritePointsPerSecond(defaultStreamMaxWritePointsPerSecond) //
                .routeTableRefreshPeriodSeconds(routeTableRefreshPeriodSeconds) //
                .authInfo(authInfo) //
                .router(router) //
                .build();

        Assert.assertEquals(database, opts.getDatabase());
        Assert.assertArrayEquals(
                endpoints, opts.getEndpoints().stream().map(Endpoint::toString).toArray());
        Assert.assertEquals(rpcOptions, opts.getRpcOptions());
        Assert.assertEquals(tlsOptions, opts.getRpcOptions().getTlsOptions());

        RouterOptions routerOptions = opts.getRouterOptions();
        Assert.assertNotNull(routerOptions);
        Assert.assertArrayEquals(
                endpoints,
                routerOptions.getEndpoints().stream().map(Endpoint::toString).toArray());
        Assert.assertEquals(router, routerOptions.getRouter());
        Assert.assertEquals(routeTableRefreshPeriodSeconds, routerOptions.getRefreshPeriodSeconds());

        WriteOptions writeOptions = opts.getWriteOptions();
        Assert.assertNotNull(writeOptions);
        Assert.assertEquals(asyncPool, writeOptions.getAsyncPool());
        Assert.assertEquals(writeMaxRetries, writeOptions.getMaxRetries());
        Assert.assertEquals(maxInFlightWritePoints, writeOptions.getMaxInFlightWritePoints());
        Assert.assertEquals(limitedPolicy, writeOptions.getLimitedPolicy());
        Assert.assertEquals(
                defaultStreamMaxWritePointsPerSecond, writeOptions.getDefaultStreamMaxWritePointsPerSecond());
        Assert.assertEquals(authInfo, writeOptions.getAuthInfo());
    }

    private Router<Void, Endpoint> createTestRouter() {
        return new Router<Void, Endpoint>() {

            @Override
            public CompletableFuture<Endpoint> routeFor(Void request) {
                return null;
            }

            @Override
            public CompletableFuture<Boolean> refresh() {
                return null;
            }

            @Override
            public void onRefresh(List<Endpoint> endpoints) {}
        };
    }
}
