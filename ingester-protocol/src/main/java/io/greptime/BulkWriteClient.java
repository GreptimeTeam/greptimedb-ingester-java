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
import com.codahale.metrics.Timer;
import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Keys;
import io.greptime.common.Lifecycle;
import io.greptime.common.util.Clock;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.MetricExecutor;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SerializingExecutor;
import io.greptime.limit.AbstractLimiter;
import io.greptime.limit.LimitedPolicy;
import io.greptime.models.ArrowHelper;
import io.greptime.models.AuthInfo;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.options.BulkWriteOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.TlsOptions;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for bulk writing data to GreptimeDB.
 * This client provides high-performance data ingestion capabilities through Arrow Flight protocol.
 */
public class BulkWriteClient implements BulkWrite, Health, Lifecycle<BulkWriteOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteClient.class);

    private BulkWriteOptions opts;
    private RouterClient routerClient;
    private Executor asyncPool;

    @Override
    public boolean init(BulkWriteOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "null `BulkWriteClient.opts`");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("bulk_write_client");
        this.asyncPool = new MetricExecutor(this.asyncPool, "async_bulk_write_pool");
        return true;
    }

    @Override
    public void shutdownGracefully() {
        // NO-OP
    }

    @Override
    public BulkStreamWriter bulkStreamWriter(
            TableSchema schema,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            long timeoutMsPerMessage,
            int maxRequestsInFlight,
            Context ctx) {
        return this.routerClient
                .route()
                .thenApply(endpoint -> bulkStreamWriteTo(
                        endpoint,
                        schema,
                        allocatorInitReservation,
                        allocatorMaxAllocation,
                        timeoutMsPerMessage,
                        maxRequestsInFlight,
                        ctx))
                .join();
    }

    /**
     * Creates a BulkStreamWriter for the specified endpoint and schema.
     *
     * @param endpoint the target server endpoint
     * @param schema the table schema for the data to be written
     * @param allocatorInitReservation initial memory reservation for Arrow allocator
     * @param allocatorMaxAllocation maximum memory allocation for Arrow allocator
     * @param timeoutMsPerMessage timeout in milliseconds for each message
     * @param maxRequestsInFlight maximum number of concurrent requests
     * @param ctx context containing additional parameters like compression
     * @return a BulkStreamWriter instance
     */
    private BulkStreamWriter bulkStreamWriteTo(
            Endpoint endpoint,
            TableSchema schema,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            long timeoutMsPerMessage,
            int maxRequestsInFlight,
            Context ctx) {
        // Creates the bulk write manager
        TlsOptions tlsOptions = this.opts.getTlsOptions();

        Schema arrowSchema = ArrowHelper.createSchema(schema);
        ArrowCompressionType compressionType = ArrowHelper.getArrowCompressionType(ctx);

        BulkWriteManager manager = BulkWriteManager.create(
                endpoint, allocatorInitReservation, allocatorMaxAllocation, compressionType, tlsOptions);

        // Creates the bulk write service
        String database = this.opts.getDatabase();
        String table = schema.getTableName();
        AuthInfo authInfo = this.opts.getAuthInfo();
        FlightCallHeaders headers = new FlightCallHeaders();
        ctx.entrySet().forEach(e -> headers.insert(e.getKey(), String.valueOf(e.getValue())));
        headers.insert(Keys.Headers.GREPTIMEDB_DBNAME, database);
        if (authInfo != null) {
            headers.insert(Keys.Headers.GREPTIMEDB_AUTH, authInfo.base64HeaderValue());
        }
        HeaderCallOption headerOption = new HeaderCallOption(headers);
        AsyncExecCallOption execOption = new AsyncExecCallOption(this.asyncPool);

        BulkWriteService writer = manager.intoBulkWriteStream(
                table, arrowSchema, timeoutMsPerMessage, maxRequestsInFlight, headerOption, execOption);
        writer.start();
        if (this.opts.isUseZeroCopyWrite()) {
            writer.tryUseZeroCopyWrite();
        }
        return new DefaultBulkStreamWriter(writer, schema, maxRequestsInFlight);
    }

    @Override
    public CompletableFuture<Map<Endpoint, Boolean>> checkHealth() {
        return this.routerClient.checkHealth();
    }

    @Override
    public void display(Printer out) {
        out.println("--- BulkWriteClient ---").print("asyncPool=").println(this.asyncPool);
    }

    @Override
    public String toString() {
        return "BulkWriteClient{" + "opts=" + opts + ", routerClient=" + routerClient + ", asyncPool=" + asyncPool
                + '}';
    }

    /**
     * Helper class for metrics collection in bulk write operations.
     */
    static final class InnerMetricHelper {
        static final Timer BULK_WRITE_PREPARE_TIME = MetricsUtil.timer("bulk_write_prepare_time");
        static final Timer BULK_WRITE_PUT_TIME = MetricsUtil.timer("bulk_write_put_time");
        static final Histogram BULK_WRITE_PUT_ROWS = MetricsUtil.histogram("bulk_write_put_rows");
        static final Histogram BULK_WRITE_PUT_BYTES = MetricsUtil.histogram("bulk_write_put_bytes");

        static Timer prepareTime() {
            return BULK_WRITE_PREPARE_TIME;
        }

        static Timer putTime() {
            return BULK_WRITE_PUT_TIME;
        }

        static Histogram putRows() {
            return BULK_WRITE_PUT_ROWS;
        }

        static Histogram putBytes() {
            return BULK_WRITE_PUT_BYTES;
        }
    }

    /**
     * Default implementation of BulkStreamWriter that manages the writing process
     * and enforces limits on concurrent requests.
     */
    static class DefaultBulkStreamWriter implements BulkStreamWriter {

        private final BulkWriteLimiter pipelineWriteLimiter;
        private final BulkWriteService writer;
        private final TableSchema tableSchema;
        private final AtomicReference<Table.TableBufferRoot> current = new AtomicReference<>();

        public DefaultBulkStreamWriter(BulkWriteService writer, TableSchema tableSchema, int maxRequestsInFlight) {
            this.writer = writer;
            this.tableSchema = tableSchema;
            this.pipelineWriteLimiter = new BulkWriteLimiter(maxRequestsInFlight);
        }

        @Override
        public Table.TableBufferRoot tableBufferRoot(int columnBufferSize) {
            Table.TableBufferRoot table =
                    Table.tableBufferRoot(this.tableSchema, this.writer.getRoot(), columnBufferSize);
            this.current.set(table);
            return table;
        }

        @Override
        public CompletableFuture<Integer> writeNext() throws Exception {
            Table.TableBufferRoot table = this.current.getAndSet(null);
            if (table == null) {
                return Util.errorCf(
                        new IllegalStateException("No table buffer available - call `tableBufferRoot()` first"));
            }
            // make sure the table is completed
            table.complete();

            String tableName = table.tableName();
            int rows = table.rowCount();
            long bytes = table.bytesUsed();

            InnerMetricHelper.putRows().update(rows);
            InnerMetricHelper.putBytes().update(bytes);

            // Check if the stream is ready
            if (!isStreamReady()) {
                LOG.debug(
                        "Stream busy with pending requests. Check `isStreamReady()` before calling `writeNext()` to avoid busy-waiting.");
            }

            return this.pipelineWriteLimiter.acquireAndDo(null, () -> {
                Clock clock = Clock.defaultClock();

                long startPut = clock.getTick();
                BulkWriteService.PutStage stage = this.writer.putNext();
                InnerMetricHelper.prepareTime().update(clock.duration(startPut), TimeUnit.MILLISECONDS);

                long startCall = clock.getTick();
                int inFlight = stage.numInFlight();
                CompletableFuture<Integer> future = stage.future();
                future.whenComplete((r, t) -> {
                    long duration = clock.duration(startCall);
                    InnerMetricHelper.putTime().update(duration, TimeUnit.MILLISECONDS);
                    if (Util.isBulkWriteLogging()) {
                        LOG.info(
                                "Bulk write completed - table={}, rows={}, bytes={}, duration={}ms, in-flight={} requests",
                                tableName,
                                rows,
                                bytes,
                                duration,
                                inFlight);
                    }
                });

                return future;
            });
        }

        @Override
        public void completed() throws Exception {
            this.writer.completed();
            this.writer.waitServerCompleted();
            this.writer.close();
        }

        @Override
        public boolean isStreamReady() {
            return this.writer.isStreamReady();
        }

        @Override
        public void close() throws Exception {
            this.writer.close();
        }
    }

    /**
     * Limiter that controls the number of concurrent bulk write operations.
     * Uses a blocking policy to ensure the maximum number of in-flight requests is not exceeded.
     */
    static class BulkWriteLimiter extends AbstractLimiter<Void, Integer> {

        public BulkWriteLimiter(int maxInFlight) {
            super(maxInFlight, new LimitedPolicy.BlockingPolicy(), "bulk_write_limiter_acquire");
        }

        @Override
        public int calculatePermits(Void in) {
            return 1;
        }

        @Override
        public Integer rejected(Void in, RejectedState state) {
            throw new IllegalStateException("A blocking limiter should never get here");
        }
    }
}
