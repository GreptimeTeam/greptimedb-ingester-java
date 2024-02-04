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

import io.greptime.common.util.SerializingExecutor;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.RpcOptions;

/**
 * @author jiachun.fjc
 */
public class TestConnector {

    public static GreptimeDB connectToDefaultDB() {
        // GreptimeDB has a default database named "public" in the default catalog "greptime",
        // we can use it as the test database
        String database = "public";
        // By default, GreptimeDB listens on port 4001 using the gRPC protocol.
        // We can provide multiple endpoints that point to the same GreptimeDB cluster.
        // The client will make calls to these endpoints based on a load balancing strategy.
        String[] endpoints = {"127.0.0.1:4001"};
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database) //
                // Optional, the default value is fine.
                //
                // Asynchronous thread pool, which is used to handle various asynchronous
                // tasks in the SDK (You are using a purely asynchronous SDK). If you do not
                // set it, there will be a default implementation, which you can reconfigure
                // if the default implementation is not satisfied.
                // The default implementation is: `SerializingExecutor`
                .asyncPool(new SerializingExecutor("async_pool"))
                // Optional, the default value is fine.
                //
                // Sets the RPC options, in general, the default configuration is fine.
                .rpcOptions(RpcOptions.newDefault())
                // Optional, the default value is fine.
                //
                // In some case of failure, a retry of write can be attempted.
                // The default is 1
                .writeMaxRetries(1)
                // Optional, the default value is fine.
                //
                // Write flow limit: maximum number of data points in-flight. It does not take effect on `StreamWriter`
                // The default is 10 * 65536
                .maxInFlightWritePoints(10 * 65536)
                // Optional, the default value is fine.
                //
                // Write flow limit: the policy to use when the write flow limit is exceeded.
                // All options:
                // - `LimitedPolicy.DiscardPolicy`: discard the data if the limiter is full.
                // - `LimitedPolicy.AbortPolicy`: abort if the limiter is full.
                // - `LimitedPolicy.BlockingPolicy`: blocks if the limiter is full.
                // - `LimitedPolicy.AbortOnBlockingTimeoutPolicy`: blocks the specified time if the limiter is full.
                // The default is `LimitedPolicy.AbortOnBlockingTimeoutPolicy`
                .writeLimitedPolicy(LimitedPolicy.defaultWriteLimitedPolicy())
                // Optional, the default value is fine.
                //
                // The default rate limit for stream writer. It only takes effect when we do not specify the
                // `maxPointsPerSecond` when creating a `StreamWriter`.
                // The default is 10 * 65536
                .defaultStreamMaxWritePointsPerSecond(10 * 65536)
                // Optional, the default value is fine.
                //
                // Refresh frequency of route tables. The background refreshes all route tables
                // periodically. By default, the route tables will not be refreshed.
                .routeTableRefreshPeriodSeconds(-1)
                // Optional, the default value is fine.
                //
                // Sets the request router, The internal default implementation works well.
                // You don't need to set it unless you have special requirements.
                .router(null)
                // Sets authentication information. If the DB is not required to authenticate, we can ignore this.
                .authInfo(AuthInfo.noAuthorization())
                // A good start ^_^
                .build();

        return GreptimeDB.create(opts);
    }
}
