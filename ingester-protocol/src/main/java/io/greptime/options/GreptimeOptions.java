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
    private BulkWriteOptions bulkWriteOptions;
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

    public BulkWriteOptions getBulkWriteOptions() {
        return bulkWriteOptions;
    }

    public void setBulkWriteOptions(BulkWriteOptions bulkWriteOptions) {
        this.bulkWriteOptions = bulkWriteOptions;
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
                + writeOptions + ", bulkWriteOptions="
                + bulkWriteOptions + ", database='"
                + database + '\'' + '}';
    }

    public static GreptimeOptions checkSelf(GreptimeOptions opts) {
        Ensures.ensureNonNull(opts, "null `opts (GreptimeOptions)`)`");
        Ensures.ensureNonNull(opts.getEndpoints(), "null `endpoints`");
        Ensures.ensure(!opts.getEndpoints().isEmpty(), "empty `endpoints`");
        Ensures.ensureNonNull(opts.getRpcOptions(), "null `rpcOptions`");
        Ensures.ensureNonNull(opts.getRouterOptions(), "null `routerOptions`");
        Ensures.ensureNonNull(opts.getWriteOptions(), "null `writeOptions`");
        Ensures.ensureNonNull(opts.getBulkWriteOptions(), "null `bulkWriteOptions`");
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
        // Use zero copy write in bulk write
        private boolean useZeroCopyWriteInBulkWrite = true;
        // Refresh frequency of route tables. The background refreshes all route tables periodically.
        // If the value is less than or equal to 0, the route tables will not be refreshed.
        private long routeTableRefreshPeriodSeconds = DEFAULT_ROUTE_TABLE_REFRESH_PERIOD_SECONDS;
        // Timeout for health check
        private long checkHealthTimeoutMs = DEFAULT_CHECK_HEALTH_TIMEOUT_MS;
        // Authentication information
        private AuthInfo authInfo;
        // The request router
        private Router<Void, Endpoint> router;

        /**
         * Create a new builder with the given endpoints and database.
         *
         * @param endpoints the endpoints to connect to
         * @param database The target database name. Default is "public". When a database name is provided,
         *                 the DB will attempt to parse catalog and schema from it. The format is
         *                 {@code [<catalog>-]<schema>}, where:
         *                 <ul>
         *                   <li>If {@code [<catalog>-]} is not provided, the entire database name is used as schema</li>
         *                   <li>If {@code [<catalog>-]} is provided, the name is split on "-" into catalog and schema</li>
         *                 </ul>
         */
        public Builder(List<Endpoint> endpoints, String database) {
            this.endpoints.addAll(endpoints);
            this.database = database;
        }

        /**
         * Sets the asynchronous thread pool used to handle various asynchronous tasks in the SDK.
         * This SDK is purely asynchronous. If not set, a default implementation will be used.
         * It's generally recommended to use the default configuration.
         *
         * <p>Default: {@link io.greptime.common.util.SerializingExecutor} - This executor does not start any additional threads,
         * it only uses the current thread to batch process small tasks.
         *
         * <p>Note: The SDK treats the thread pool as a shared resource and will not actively close it
         * to release resources (even if it needs to be closed).
         *
         * @param asyncPool the asynchronous thread pool to use
         * @return this builder
         * @see io.greptime.common.util.SerializingExecutor
         */
        public Builder asyncPool(Executor asyncPool) {
            this.asyncPool = asyncPool;
            return this;
        }

        /**
         * Sets the RPC options. In general, the default configuration is fine.
         *
         * <p>This configuration only applies to Regular API, not to Bulk API.
         *
         * <p>Key parameters include:
         * <ul>
         *   <li>useRpcSharedPool: Whether to use global RPC shared pool (default: false)</li>
         *   <li>defaultRpcTimeout: RPC request timeout (default: 60000ms)</li>
         *   <li>maxInboundMessageSize: Maximum inbound message size (default: 256MB)</li>
         *   <li>flowControlWindow: Flow control window size (default: 256MB)</li>
         *   <li>idleTimeoutSeconds: Idle timeout duration (default: 5 minutes)</li>
         *   <li>keepAliveTimeSeconds: Keep-alive ping interval (default: disabled)</li>
         *   <li>limitKind: gRPC layer concurrency limit algorithm (default: None)</li>
         * </ul>
         *
         * @param rpcOptions the RPC options
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
         * <p>Key parameters include:
         * <ul>
         *   <li>clientCertChain: Client certificate chain file</li>
         *   <li>privateKey: Private key file</li>
         *   <li>privateKeyPassword: Private key password</li>
         *   <li>rootCerts: Root certificate file</li>
         * </ul>
         *
         * @param tlsOptions TLS options for secure connection configuration, set to null to use plaintext
         * @return this builder
         */
        public Builder tlsOptions(TlsOptions tlsOptions) {
            this.tlsOptions = tlsOptions;
            return this;
        }

        /**
         * Maximum number of retries for write failures. Whether to retry depends on the error type
         * {@link io.greptime.Status#isShouldRetry()}.
         *
         * <p>This configuration only applies to Regular API, not to Bulk API.
         *
         * <p>Default: 1
         *
         * @param maxRetries the maximum number of retry attempts
         * @return this builder
         */
        public Builder writeMaxRetries(int maxRetries) {
            this.writeMaxRetries = maxRetries;
            return this;
        }

        /**
         * Write flow limit: maximum number of data points in-flight.
         *
         * <p>This configuration only applies to Regular API, not to Bulk API.
         *
         * <p>Default: 655360 (10 * 65536)
         *
         * @param maxInFlightWritePoints the maximum number of in-flight write points
         * @return this builder
         */
        public Builder maxInFlightWritePoints(int maxInFlightWritePoints) {
            this.maxInFlightWritePoints = maxInFlightWritePoints;
            return this;
        }

        /**
         * Write flow limit: the policy to use when the write flow limit is exceeded.
         *
         * <p>This configuration only applies to Regular API, not to Bulk API.
         *
         * <p>Available policies:
         * <ul>
         *   <li>{@code DiscardPolicy}: Discard data if limiter is full</li>
         *   <li>{@code AbortPolicy}: Abort if limiter is full, throw exception</li>
         *   <li>{@code BlockingPolicy}: Block the write thread if limiter is full</li>
         *   <li>{@code BlockingTimeoutPolicy}: Block for specified time then proceed if limiter is full</li>
         *   <li>{@code AbortOnBlockingTimeoutPolicy}: Block for specified time, abort and throw exception if timeout</li>
         * </ul>
         *
         * <p>Default: {@code AbortOnBlockingTimeoutPolicy} (3 seconds)
         *
         * @param writeLimitedPolicy the write flow control policy
         * @return this builder
         */
        public Builder writeLimitedPolicy(LimitedPolicy writeLimitedPolicy) {
            this.writeLimitedPolicy = writeLimitedPolicy;
            return this;
        }

        /**
         * The default rate limit value (points per second) for `StreamWriter`. It only takes
         * effect when we do not specify the `maxPointsPerSecond` when creating a `StreamWriter`.
         *
         * <p>This configuration only applies to Regular API, not to Bulk API.
         *
         * <p>Default: 655360 (10 * 65536)
         *
         * @param defaultStreamMaxWritePointsPerSecond the default maximum write points per second for StreamWriter
         * @return this builder
         */
        public Builder defaultStreamMaxWritePointsPerSecond(int defaultStreamMaxWritePointsPerSecond) {
            this.defaultStreamMaxWritePointsPerSecond = defaultStreamMaxWritePointsPerSecond;
            return this;
        }

        /**
         * Whether to use zero-copy optimization in bulk write operations.
         *
         * <p>This configuration is effective for Bulk API.
         *
         * <p>Default: true
         *
         * @param useZeroCopyWriteInBulkWrite whether to use zero-copy write optimization in bulk operations
         * @return this builder
         */
        public Builder useZeroCopyWriteInBulkWrite(boolean useZeroCopyWriteInBulkWrite) {
            this.useZeroCopyWriteInBulkWrite = useZeroCopyWriteInBulkWrite;
            return this;
        }

        /**
         * Background refresh period for route tables (seconds). The background refreshes all route tables
         * periodically. Route tables will not be refreshed if value is <= 0.
         *
         * <p>Default: 600 (10 minutes)
         *
         * @param routeTableRefreshPeriodSeconds the refresh period for route tables cache in seconds
         * @return this builder
         */
        public Builder routeTableRefreshPeriodSeconds(long routeTableRefreshPeriodSeconds) {
            this.routeTableRefreshPeriodSeconds = routeTableRefreshPeriodSeconds;
            return this;
        }

        /**
         * Timeout for health check operations (milliseconds).
         * If the health check is not completed within the specified time, the health
         * check will fail.
         *
         * <p>Default: 1000 (1 second)
         *
         * @param checkHealthTimeoutMs the timeout for health check operations in milliseconds
         * @return this builder
         */
        public Builder checkHealthTimeoutMs(long checkHealthTimeoutMs) {
            this.checkHealthTimeoutMs = checkHealthTimeoutMs;
            return this;
        }

        /**
         * Sets database authentication information. Can be ignored if database doesn't require authentication.
         *
         * <p>Default: null
         *
         * @param authInfo the database authentication information
         * @return this builder
         */
        public Builder authInfo(AuthInfo authInfo) {
            this.authInfo = authInfo;
            return this;
        }

        /**
         * Sets custom request router. The internal default implementation works well.
         * No need to set unless you have special requirements.
         *
         * <p>Default: Internal default implementation
         *
         * @param router the custom request router implementation
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
            opts.setBulkWriteOptions(bulkWriteOptions());
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

        private BulkWriteOptions bulkWriteOptions() {
            BulkWriteOptions bulkWriteOpts = new BulkWriteOptions();
            bulkWriteOpts.setDatabase(this.database);
            bulkWriteOpts.setAuthInfo(this.authInfo);
            bulkWriteOpts.setAsyncPool(this.asyncPool);
            bulkWriteOpts.setUseZeroCopyWrite(this.useZeroCopyWriteInBulkWrite);
            bulkWriteOpts.setTlsOptions(this.tlsOptions);
            return bulkWriteOpts;
        }
    }
}
