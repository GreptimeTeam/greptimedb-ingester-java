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

import io.greptime.RouterClient;
import io.greptime.common.Copiable;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import java.util.concurrent.Executor;

/**
 * Write options.
 */
public class WriteOptions implements Copiable<WriteOptions> {
    private String database;
    private AuthInfo authInfo;
    private RouterClient routerClient;
    private Executor asyncPool;
    private int maxRetries = 1;
    // Write flow limit: maximum number of data rows in-flight.
    private int maxInFlightWritePoints = 10 * 65536;
    private LimitedPolicy limitedPolicy = LimitedPolicy.defaultWriteLimitedPolicy();
    // Default rate limit for stream writer
    private int defaultStreamMaxWritePointsPerSecond = 10 * 65536;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public AuthInfo getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = authInfo;
    }

    public RouterClient getRouterClient() {
        return routerClient;
    }

    public void setRouterClient(RouterClient routerClient) {
        this.routerClient = routerClient;
    }

    public Executor getAsyncPool() {
        return asyncPool;
    }

    public void setAsyncPool(Executor asyncPool) {
        this.asyncPool = asyncPool;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getMaxInFlightWritePoints() {
        return maxInFlightWritePoints;
    }

    public void setMaxInFlightWritePoints(int maxInFlightWritePoints) {
        this.maxInFlightWritePoints = maxInFlightWritePoints;
    }

    public LimitedPolicy getLimitedPolicy() {
        return limitedPolicy;
    }

    public void setLimitedPolicy(LimitedPolicy limitedPolicy) {
        this.limitedPolicy = limitedPolicy;
    }

    public int getDefaultStreamMaxWritePointsPerSecond() {
        return defaultStreamMaxWritePointsPerSecond;
    }

    public void setDefaultStreamMaxWritePointsPerSecond(int defaultStreamMaxWritePointsPerSecond) {
        this.defaultStreamMaxWritePointsPerSecond = defaultStreamMaxWritePointsPerSecond;
    }

    @Override
    public WriteOptions copy() {
        WriteOptions opts = new WriteOptions();
        opts.database = this.database;
        opts.authInfo = this.authInfo;
        opts.routerClient = this.routerClient;
        opts.asyncPool = this.asyncPool;
        opts.maxRetries = this.maxRetries;
        opts.maxInFlightWritePoints = this.maxInFlightWritePoints;
        opts.limitedPolicy = this.limitedPolicy;
        opts.defaultStreamMaxWritePointsPerSecond = this.defaultStreamMaxWritePointsPerSecond;
        return opts;
    }

    @Override
    public String toString() {
        // Do not print auto info
        return "WriteOptions{" + "database='"
                + database + '\'' + ", routerClient="
                + routerClient + ", asyncPool="
                + asyncPool + ", maxRetries="
                + maxRetries + ", maxInFlightWritePoints="
                + maxInFlightWritePoints + ", limitedPolicy="
                + limitedPolicy + ", defaultStreamMaxWritePointsPerSecond="
                + defaultStreamMaxWritePointsPerSecond + '}';
    }
}
