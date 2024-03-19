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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.RateLimiter;
import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Keys;
import io.greptime.common.Lifecycle;
import io.greptime.common.util.Clock;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.MetricExecutor;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SerializingExecutor;
import io.greptime.errors.LimitedException;
import io.greptime.errors.ServerException;
import io.greptime.errors.StreamException;
import io.greptime.limit.LimitedPolicy;
import io.greptime.limit.WriteLimiter;
import io.greptime.models.AuthInfo;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableHelper;
import io.greptime.models.WriteOk;
import io.greptime.models.WriteTables;
import io.greptime.options.WriteOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.Observer;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Default Write API impl.
 *
 * @author jiachun.fjc
 */
public class WriteClient implements Write, Lifecycle<WriteOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(WriteClient.class);

    private WriteOptions opts;
    private RouterClient routerClient;
    private Executor asyncPool;
    private WriteLimiter writeLimiter;

    @Override
    public boolean init(WriteOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "null `WriteClient.opts`");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("write_client");
        this.asyncPool = new MetricExecutor(this.asyncPool, "async_write_pool.time");
        this.writeLimiter =
                new DefaultWriteLimiter(this.opts.getMaxInFlightWritePoints(), this.opts.getLimitedPolicy());
        return true;
    }

    @Override
    public void shutdownGracefully() {
        // NO-OP
    }

    @Override
    public CompletableFuture<Result<WriteOk, Err>> write(Collection<Table> tables, WriteOp writeOp, Context ctx) {
        Ensures.ensureNonNull(tables, "null `tables`");
        Ensures.ensure(!tables.isEmpty(), "empty `tables`");

        long startCall = Clock.defaultClock().getTick();
        WriteTables writeTables = new WriteTables(tables, writeOp);
        return this.writeLimiter.acquireAndDo(tables, () -> write0(writeTables, ctx, 0).whenCompleteAsync((r, e) -> {
            InnerMetricHelper.writeQps().mark();
            if (r != null) {
                if (Util.isRwLogging()) {
                    LOG.info("Write to {} with operation {}, duration={} ms, result={}.",
                            Keys.DB_NAME,
                            writeOp,
                            Clock.defaultClock().duration(startCall),
                            r
                    );
                }
                if (r.isOk()) {
                    WriteOk ok = r.getOk();
                    InnerMetricHelper.writeRowsSuccessNum(writeOp).update(ok.getSuccess());
                    InnerMetricHelper.writeRowsFailureNum(writeOp).update(ok.getFailure());
                    return;
                }
            }
            InnerMetricHelper.writeFailureNum().mark();
        }, this.asyncPool));
    }

    @Override
    public StreamWriter<Table, WriteOk> streamWriter(int maxPointsPerSecond, Context ctx) {
        int permitsPerSecond = maxPointsPerSecond > 0 ? maxPointsPerSecond : this.opts.getDefaultStreamMaxWritePointsPerSecond();

        CompletableFuture<WriteOk> respFuture = new CompletableFuture<>();

        return this.routerClient.route()
                .thenApply(endpoint -> streamWriteTo(endpoint, ctx, Util.toObserver(respFuture)))
                .thenApply(reqObserver -> new RateLimitingStreamWriter(reqObserver, permitsPerSecond) {

                    @Override
                    public StreamWriter<Table, WriteOk> write(Table table, WriteOp writeOp) {
                        if (respFuture.isCompletedExceptionally()) {
                            respFuture.getNow(null); // throw the exception now
                        }
                        return super.write(table, writeOp); // may wait
                    }

                    @Override
                    public CompletableFuture<WriteOk> completed() {
                        reqObserver.onCompleted();
                        return respFuture;
                    }
                }).join();
    }

    private CompletableFuture<Result<WriteOk, Err>> write0(WriteTables writeTables, Context ctx, int retries) {
        InnerMetricHelper.writeByRetries(retries).mark();

        return this.routerClient.route()
            .thenComposeAsync(endpoint -> writeTo(endpoint, writeTables, ctx, retries), this.asyncPool)
            .thenComposeAsync(r -> {
                if (r.isOk()) {
                    LOG.debug("Success to write to {}, ok={}.", Keys.DB_NAME, r.getOk());
                    return Util.completedCf(r);
                }

                Err err = r.getErr();
                LOG.warn("Failed to write to {}, retries={}, err={}.", Keys.DB_NAME, retries, err);
                if (retries + 1 > this.opts.getMaxRetries()) {
                    LOG.error("Retried {} times still failed.", retries);
                    return Util.completedCf(r);
                }

                if (Util.shouldNotRetry(err)) {
                    return Util.completedCf(r);
                }

                return write0(writeTables, ctx, retries + 1);
            }, this.asyncPool);
    }

    private CompletableFuture<Result<WriteOk, Err>> writeTo(Endpoint endpoint, WriteTables writeTables, Context ctx, int retries) {
        // Some info will be set into the GreptimeDB Request header.
        String database = this.opts.getDatabase();
        AuthInfo authInfo = this.opts.getAuthInfo();

        Database.GreptimeRequest req = TableHelper.toGreptimeRequest(writeTables, database, authInfo);
        ctx.with("retries", retries);

        CompletableFuture<Database.GreptimeResponse> future = this.routerClient.invoke(endpoint, req, ctx);

        return future.thenApplyAsync(resp -> {
            Common.ResponseHeader header = resp.getHeader();
            Common.Status status = header.getStatus();
            int statusCode = status.getStatusCode();
            if (Status.isSuccess(statusCode)) {
                int affectedRows = resp.getAffectedRows().getValue();
                return WriteOk.ok(affectedRows, 0).mapToResult();
            } else {
                return Err.writeErr(statusCode, new ServerException(status.getErrMsg()), endpoint).mapToResult();
            }
        }, this.asyncPool);
    }

    private Observer<WriteTables> streamWriteTo(Endpoint endpoint, Context ctx, Observer<WriteOk> respObserver) {
        Observer<Database.GreptimeRequest> rpcObserver =
                this.routerClient.invokeClientStreaming(endpoint, Database.GreptimeRequest.getDefaultInstance(), ctx,
                        new Observer<Database.GreptimeResponse>() {

                            @Override
                            public void onNext(Database.GreptimeResponse resp) {
                                int affectedRows = resp.getAffectedRows().getValue();
                                Result<WriteOk, Err> ret = WriteOk.ok(affectedRows, 0).mapToResult();
                                if (ret.isOk()) {
                                    respObserver.onNext(ret.getOk());
                                } else {
                                    respObserver.onError(new StreamException(String.valueOf(ret.getErr())));
                                }
                            }

                            @Override
                            public void onError(Throwable err) {
                                respObserver.onError(err);
                            }

                            @Override
                            public void onCompleted() {
                                respObserver.onCompleted();
                            }
                        });

        // Some info will be set into the GreptimeDB Request header.
        String database = this.opts.getDatabase();
        AuthInfo authInfo = this.opts.getAuthInfo();

        return new Observer<WriteTables>() {

            @Override
            public void onNext(WriteTables writeTables) {
                Database.GreptimeRequest req = TableHelper.toGreptimeRequest(writeTables, database, authInfo);
                rpcObserver.onNext(req);
            }

            @Override
            public void onError(Throwable err) {
                rpcObserver.onError(err);
            }

            @Override
            public void onCompleted() {
                rpcObserver.onCompleted();
            }
        };
    }

    @Override
    public void display(Printer out) {
        out.println("--- WriteClient ---") //
                .print("maxRetries=") //
                .println(this.opts.getMaxRetries()) //
                .print("asyncPool=") //
                .println(this.asyncPool);
    }

    @Override
    public String toString() {
        return "WriteClient{" + //
                "opts=" + opts + //
                ", routerClient=" + routerClient + //
                ", asyncPool=" + asyncPool + //
                '}';
    }

    static final class InnerMetricHelper {
        static final Histogram INSERT_ROWS_SUCCESS_NUM = MetricsUtil.histogram("insert_rows_success_num");
        static final Histogram DELETE_ROWS_SUCCESS_NUM = MetricsUtil.histogram("delete_rows_success_num");
        static final Histogram INSERT_ROWS_FAILURE_NUM = MetricsUtil.histogram("insert_rows_failure_num");
        static final Histogram DELETE_ROWS_FAILURE_NUM = MetricsUtil.histogram("delete_rows_failure_num");
        static final Timer WRITE_STREAM_LIMITER_ACQUIRE_WAIT_TIME = MetricsUtil
                .timer("write_stream_limiter_acquire_wait_time");
        static final Meter WRITE_FAILURE_NUM = MetricsUtil.meter("write_failure_num");
        static final Meter WRITE_QPS = MetricsUtil.meter("write_qps");

        static Histogram writeRowsSuccessNum(WriteOp writeOp) {
            switch (writeOp) {
                case Insert:
                    return INSERT_ROWS_SUCCESS_NUM;
                case Delete:
                    return DELETE_ROWS_SUCCESS_NUM;
                default:
                    throw new IllegalArgumentException("Unsupported write operation: " + writeOp);
            }
        }

        static Histogram writeRowsFailureNum(WriteOp writeOp) {
            switch (writeOp) {
                case Insert:
                    return INSERT_ROWS_FAILURE_NUM;
                case Delete:
                    return DELETE_ROWS_FAILURE_NUM;
                default:
                    throw new IllegalArgumentException("Unsupported write operation: " + writeOp);
            }
        }

        static Timer writeStreamLimiterAcquireWaitTime() {
            return WRITE_STREAM_LIMITER_ACQUIRE_WAIT_TIME;
        }

        static Meter writeFailureNum() {
            return WRITE_FAILURE_NUM;
        }

        static Meter writeQps() {
            return WRITE_QPS;
        }

        static Meter writeByRetries(int retries) {
            // more than 3 retries are classified as the same metric
            return MetricsUtil.meter("write_by_retries", Math.min(3, retries));
        }
    }

    static class DefaultWriteLimiter extends WriteLimiter {

        public DefaultWriteLimiter(int maxInFlight, LimitedPolicy policy) {
            super(maxInFlight, policy, "write_limiter_acquire");
        }

        @Override
        public int calculatePermits(Collection<Table> in) {
            return in.stream().map(Table::pointCount).reduce(0, Integer::sum);
        }

        @Override
        public Result<WriteOk, Err> rejected(Collection<Table> in, RejectedState state) {
            String errMsg =
                    String.format("Write limited by client, acquirePermits=%d, maxPermits=%d, availablePermits=%d.", //
                            state.acquirePermits(), //
                            state.maxPermits(), //
                            state.availablePermits());
            return Result.err(Err.writeErr(Result.FLOW_CONTROL, new LimitedException(errMsg), null));
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    static abstract class RateLimitingStreamWriter implements StreamWriter<Table, WriteOk> {

        private final Observer<WriteTables> observer;
        private final RateLimiter rateLimiter;

        RateLimitingStreamWriter(Observer<WriteTables> observer, double permitsPerSecond) {
            this.observer = observer;
            if (permitsPerSecond > 0) {
                this.rateLimiter = RateLimiter.create(permitsPerSecond);
            } else {
                this.rateLimiter = null;
            }
        }

        @Override
        public StreamWriter<Table, WriteOk> write(Table table, WriteOp writeOp) {
            Ensures.ensureNonNull(table, "null `table`");

            int permits = table.pointCount();
            if (this.rateLimiter != null && permits > 0) {
                double millisToWait = this.rateLimiter.acquire(permits) * 1000;
                InnerMetricHelper.writeStreamLimiterAcquireWaitTime()
                        .update((long) millisToWait, TimeUnit.MILLISECONDS);
            }

            this.observer.onNext(new WriteTables(table, writeOp));
            return this;
        }
    }
}
