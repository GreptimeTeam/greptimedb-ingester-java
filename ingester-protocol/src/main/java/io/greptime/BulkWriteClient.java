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

import io.greptime.common.Display;
import io.greptime.common.Endpoint;
import io.greptime.common.Lifecycle;
import io.greptime.common.util.Ensures;
import io.greptime.common.util.MetricExecutor;
import io.greptime.common.util.SerializingExecutor;
import io.greptime.models.ArrowHelper;
import io.greptime.models.AuthInfo;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.options.BulkWriteOptions;
import io.greptime.rpc.Context;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkWriteClient implements BulkWrite, Health, Lifecycle<BulkWriteOptions>, Display {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteManager.class);

    private BulkWriteOptions opts;
    private RouterClient routerClient;
    private Executor asyncPool;

    @Override
    public boolean init(BulkWriteOptions opts) {
        this.opts = Ensures.ensureNonNull(opts, "null `BulkWriteClient.opts`");
        this.routerClient = this.opts.getRouterClient();
        Executor pool = this.opts.getAsyncPool();
        this.asyncPool = pool != null ? pool : new SerializingExecutor("buld_write_client");
        this.asyncPool = new MetricExecutor(this.asyncPool, "async_bulk_write_pool.time");
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
            Context ctx) {
        return this.routerClient
                .route()
                .thenApply(endpoint -> bulkStreamWriteTo(
                        endpoint, schema, allocatorInitReservation, allocatorMaxAllocation, timeoutMsPerMessage, ctx))
                .join();
    }

    private BulkStreamWriter bulkStreamWriteTo(
            Endpoint endpoint,
            TableSchema schema,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            long timeoutMsPerMessage,
            Context ctx) {
        String database = this.opts.getDatabase();
        String table = schema.getTableName();
        AuthInfo authInfo = this.opts.getAuthInfo();

        FlightCallHeaders headers = new FlightCallHeaders();
        ctx.entrySet().forEach(e -> headers.insert(e.getKey(), String.valueOf(e.getValue())));
        if (authInfo != null) {
            headers.insert("authorization-bin", authInfo.into().toByteString().toByteArray());
        }
        HeaderCallOption headerOption = new HeaderCallOption(headers);
        AsyncExecCallOption execOption = new AsyncExecCallOption(this.asyncPool);

        Schema arrowSchema = ArrowHelper.createSchema(schema);

        BulkWriteManager manager = BulkWriteManager.create(endpoint, allocatorInitReservation, allocatorMaxAllocation);
        BulkWriteService writer =
                manager.bulkWriteStream(database, table, arrowSchema, timeoutMsPerMessage, headerOption, execOption);
        if (this.opts.isUseZeroCopyWrite()) {
            writer.tryUseZeroCopyWrite();
        }
        return new DefaultBulkStreamWriter(writer, schema);
    }

    @Override
    public CompletableFuture<Map<Endpoint, Boolean>> checkHealth() {
        return this.routerClient.checkHealth();
    }

    @Override
    public void display(Printer out) {
        out.println("--- BulkWriteClient ---").print("asyncPool=").println(this.asyncPool);
    }

    static class DefaultBulkStreamWriter implements BulkStreamWriter {

        private final BulkWriteService writer;
        private final TableSchema tableSchema;

        public DefaultBulkStreamWriter(BulkWriteService writer, TableSchema tableSchema) {
            this.writer = writer;
            this.tableSchema = tableSchema;
        }

        @Override
        public Table allocateTableBuffer() {
            return Table.from(this.tableSchema, this.writer.getRoot());
        }

        @Override
        public CompletableFuture<Boolean> writeNext() {
            return this.writer.putNext();
        }

        @Override
        public void completed() {
            this.writer.completed();
            this.writer.waitServerCompleted();
        }

        @Override
        public void close() throws Exception {
            this.writer.close();
        }
    }
}
