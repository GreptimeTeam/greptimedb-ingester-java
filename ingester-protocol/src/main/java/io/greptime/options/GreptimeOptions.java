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
import io.greptime.common.Copiable;
import io.greptime.common.Endpoint;
import io.greptime.common.util.Ensures;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.AuthInfo;
import io.greptime.rpc.RpcOptions;
import io.greptime.rpc.TlsOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * GreptimeDB client options.
 */
public class GreptimeOptions implements Copiable<GreptimeOptions> {
    public static final int DEFAULT_WRITE_MAX_RETRIES = 1;
    public static final int DEFAULT_MAX_IN_FLIGHT_WRITE_POINTS = 10 * 65536;
    public static final int DEFAULT_DEFAULT_STREAM_MAX_WRITE_POINTS_PER_SECOND = 10 * 65536;
    public static final long DEFAULT_ROUTE_TABLE_REFRESH_PERIOD_SECONDS = 10 * 60;
    public static final long DEFAULT_CHECK_HEALTH_TIMEOUT_MS = 1000;

    private List<Endpoint> endpoints;
    private RpcOptions rpcOptions;
    private RouterOptions routerOptions;
    private WriteOptions writeOptions;
    private String database;

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    public void setRpcOptions(RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
    }

    public RouterOptions getRouterOptions() {
        return routerOptions;
    }

    public void setRouterOptions(RouterOptions routerOptions) {
        this.routerOptions = routerOptions;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    public void setWriteOptions(WriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public GreptimeOptions copy() {
        GreptimeOptions opts = new GreptimeOptions();
        opts.endpoints = new ArrayList<>(this.endpoints);
        opts.database = this.database;
        if (this.rpcOptions != null) {
            opts.rpcOptions = this.rpcOptions.copy();
        }
        if (this.routerOptions != null) {
            opts.routerOptions = this.routerOptions.copy();
        }
        if (this.writeOptions != null) {
            opts.writeOptions = this.writeOptions.copy();
        }
        return opts;
    }

    @Override
    public String toString() {
        return "GreptimeOptions{" + "endpoints="
                + endpoints + ", rpcOptions="
                + rpcOptions + ", routerOptions="
                + routerOptions + ", writeOptions="
                + writeOptions + ", database='"
                + database + '\'' + '}';
    }

    public static GreptimeOptions checkSelf(GreptimeOptions opts) {
        Ensures.ensureNonNull(opts, "null `opts (GreptimeOptions)`)`");
        Ensures.ensureNonNull(opts.getEndpoints(), "null `endpoints`");
        Ensures.ensure(!opts.getEndpoints().isEmpty(), "empty `endpoints`");
        Ensures.ensureNonNull(opts.getRpcOptions(), "null `rpcOptions`");
        Ensures.ensureNonNull(opts.getRouterOptions(), "null `routerOptions`");
        Ensures.ensureNonNull(opts.getWriteOptions(), "null `writeOptions`");

        return opts;
    }

    public static Builder newBuilder(String endpoint, String database) {
        return newBuilder(Endpoint.parse(endpoint), database);
    }

    public static Builder newBuilder(Endpoint endpoint, String database) {
        return new Builder(Collections.singletonList(endpoint), database);
    }

    public static Builder newBuilder(String[] endpoints, String database) {
        return new Builder(Arrays.stream(endpoints).map(Endpoint::parse).collect(Collectors.toList()), database);
    }

    public static Builder newBuilder(Endpoint[] endpoints, String database) {
        return new Builder(Arrays.asList(endpoints), database);
    }

    public static final class Builder {
        private final List<Endpoint> endpoints = new ArrayList<>();
        private final String database;

        // Asynchronous thread pool, which is used to handle various asynchronous tasks in the SDK.
        private Executor asyncPool;
        // Rpc options, in general the default configuration is fine.
        private RpcOptions rpcOptions = RpcOptions.newDefault();
        // GreptimeDB secure connection options
        private TlsOptions tlsOptions;
        private int writeMaxRetries = DEFAULT_WRITE_MAX_RETRIES;
        // Write flow limit: maximum number of data points in-flight.
        private int maxInFlightWritePoints = DEFAULT_MAX_IN_FLIGHT_WRITE_POINTS;
        private LimitedPolicy writeLimitedPolicy = LimitedPolicy.defaultWriteLimitedPolicy();
        private int defaultStreamMaxWritePointsPerSecond = DEFAULT_DEFAULT_STREAM_MAX_WRITE_POINTS_PER_SECOND;
        // Refresh frequency of route tables. The background refreshes all route tables periodically.
        // If the value is less than or equal to 0, the route tables will not be refreshed.
        private long routeTableRefreshPeriodSeconds = DEFAULT_ROUTE_TABLE_REFRESH_PERIOD_SECONDS;
        // Timeout for health check
        private long checkHealthTimeoutMs = DEFAULT_CHECK_HEALTH_TIMEOUT_MS;
        // Authentication information
        private AuthInfo authInfo;
        // The request router
        private Router<Void, Endpoint> router;

        public Builder(List<Endpoint> endpoints, String database) {
            this.endpoints.addAll(endpoints);
            this.database = database;
        }

        /**
         * Asynchronous thread pool, which is used to handle various asynchronous
         * tasks in the SDK (You are using a purely asynchronous SDK). If you do not
         * set it, there will be a default implementation, which you can reconfigure
         * if the default implementation is not satisfied.
         * <p>
         * Note: We do not close it to free resources(if it needs to be closed), as we
         * view it as shared.
         *
         * @param asyncPool async thread pool
         * @return this builder
         */
        public Builder asyncPool(Executor asyncPool) {
            this.asyncPool = asyncPool;
            return this;
        }

        /**
         * Sets the RPC options, in general, the default configuration is fine.
         *
         * @param rpcOptions the rpc options
         * @return this builder
         */
        public Builder rpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return this;
        }

        /**
         * Set `TlsOptions` to use secure connection between client and server. Set to `null` to use
         * plaintext connection instead.
         *
         * @param tlsOptions for configure secure connection, set to null to use plaintext
         * @return this builder
         */
        public Builder tlsOptions(TlsOptions tlsOptions) {
            this.tlsOptions = tlsOptions;
            return this;
        }

        /**
         * In some case of failure, a retry of write can be attempted.
         *
         * @param maxRetries max retries times
         * @return this builder
         */
        public Builder writeMaxRetries(int maxRetries) {
            this.writeMaxRetries = maxRetries;
            return this;
        }

        /**
         * Write flow limit: maximum number of data points in-flight.
         *
         * @param maxInFlightWritePoints max in-flight points
         * @return this builder
         */
        public Builder maxInFlightWritePoints(int maxInFlightWritePoints) {
            this.maxInFlightWritePoints = maxInFlightWritePoints;
            return this;
        }

        /**
         * Write flow limit: the policy to use when the write flow limit is exceeded.
         * The options:
         *  - `LimitedPolicy.DiscardPolicy`: discard the data if the limiter is full.
         *  - `LimitedPolicy.AbortPolicy`: abort if the limiter is full.
         *  - `LimitedPolicy.BlockingPolicy`: blocks if the limiter is full.
         *  - `LimitedPolicy.AbortOnBlockingTimeoutPolicy`: blocks the specified time if
         *  the limiter is full, abort if timeout.
         * The default is `LimitedPolicy.AbortOnBlockingTimeoutPolicy`
         *
         * @param writeLimitedPolicy write limited policy
         * @return this builder
         */
        public Builder writeLimitedPolicy(LimitedPolicy writeLimitedPolicy) {
            this.writeLimitedPolicy = writeLimitedPolicy;
            return this;
        }

        /**
         * The default rate limit value(points per second) for `StreamWriter`. It only takes
         * effect when we do not specify the `maxPointsPerSecond` when creating a `StreamWriter`.
         * The default is 10 * 65536
         *
         * @param defaultStreamMaxWritePointsPerSecond default max write points per second
         * @return this builder
         */
        public Builder defaultStreamMaxWritePointsPerSecond(int defaultStreamMaxWritePointsPerSecond) {
            this.defaultStreamMaxWritePointsPerSecond = defaultStreamMaxWritePointsPerSecond;
            return this;
        }

        /**
         * Refresh frequency of route tables. The background refreshes all route tables
         * periodically. By default, By default, the route tables will not be refreshed.
         *
         * @param routeTableRefreshPeriodSeconds refresh period for route tables cache
         * @return this builder
         */
        public Builder routeTableRefreshPeriodSeconds(long routeTableRefreshPeriodSeconds) {
            this.routeTableRefreshPeriodSeconds = routeTableRefreshPeriodSeconds;
            return this;
        }

        /**
         * Timeout for health check. The default is 1000ms.
         * If the health check is not completed within the specified time, the health
         * check will fail.
         *
         * @param checkHealthTimeoutMs timeout for health check
         * @return this builder
         */
        public Builder checkHealthTimeoutMs(long checkHealthTimeoutMs) {
            this.checkHealthTimeoutMs = checkHealthTimeoutMs;
            return this;
        }

        /**
         * Sets authentication information. If the DB is not required to authenticate,
         * we can ignore this.
         *
         * @param authInfo the authentication information
         * @return this builder
         */
        public Builder authInfo(AuthInfo authInfo) {
            this.authInfo = authInfo;
            return this;
        }

        /**
         * Sets the request router. The internal default implementation works well.
         * You don't need to set it unless you have special requirements.
         *
         * @param router the request router
         * @return this builder
         */
        public Builder router(Router<Void, Endpoint> router) {
            this.router = router;
            return this;
        }

        /**
         * A good start, happy coding.
         *
         * @return nice things
         */
        public GreptimeOptions build() {
            // Set tls options to rpc options if tls options is not null
            if (this.tlsOptions != null && this.rpcOptions != null) {
                this.rpcOptions.setTlsOptions(this.tlsOptions);
            }
            GreptimeOptions opts = new GreptimeOptions();
            opts.setEndpoints(this.endpoints);
            opts.setRpcOptions(this.rpcOptions);
            opts.setDatabase(this.database);
            opts.setRouterOptions(routerOptions());
            opts.setWriteOptions(writeOptions());
            return GreptimeOptions.checkSelf(opts);
        }

        private RouterOptions routerOptions() {
            RouterOptions routerOpts = new RouterOptions();
            routerOpts.setEndpoints(this.endpoints);
            routerOpts.setRouter(this.router);
            routerOpts.setRefreshPeriodSeconds(this.routeTableRefreshPeriodSeconds);
            routerOpts.setCheckHealthTimeoutMs(this.checkHealthTimeoutMs);
            return routerOpts;
        }

        private WriteOptions writeOptions() {
            WriteOptions writeOpts = new WriteOptions();
            writeOpts.setDatabase(this.database);
            writeOpts.setAuthInfo(this.authInfo);
            writeOpts.setAsyncPool(this.asyncPool);
            writeOpts.setMaxRetries(this.writeMaxRetries);
            writeOpts.setMaxInFlightWritePoints(this.maxInFlightWritePoints);
            writeOpts.setLimitedPolicy(this.writeLimitedPolicy);
            writeOpts.setDefaultStreamMaxWritePointsPerSecond(this.defaultStreamMaxWritePointsPerSecond);
            return writeOpts;
        }
    }
}
