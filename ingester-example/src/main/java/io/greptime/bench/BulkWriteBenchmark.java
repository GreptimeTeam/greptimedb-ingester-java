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

package io.greptime.bench;

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.common.util.ServiceLoader;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkWriteBenchmark is a benchmark for the bulk write API of GreptimeDB.
 *
 * Env:
 * - db_endpoint: the endpoint of the GreptimeDB server
 * - db_name: the name of the database
 * - batch_size_per_request: the batch size per request
 * - zstd_compression: whether to use zstd compression
 */
public class BulkWriteBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteBenchmark.class);

    public static void main(String[] args) throws Exception {
        String endpoint = SystemPropertyUtil.get("db_endpoint", "127.0.0.1:4001");
        String dbName = SystemPropertyUtil.get("db_name", "public");
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", true);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 64 * 1024);
        LOG.info("Connect to db: {}, endpoint: {}", dbName, endpoint);
        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);

        GreptimeDB greptimeDB = DBConnector.connectTo(new String[] {endpoint}, dbName);

        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(4 * 1024 * 1024 * 1024L)
                .timeoutMsPerMessage(60000)
                .maxRequestsInFlight(4)
                .build();
        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        TableDataProvider tableDataProvider =
                ServiceLoader.load(TableDataProvider.class).first();
        LOG.info("Table data provider: {}", tableDataProvider.getClass().getName());
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();

        LOG.info("Start writing data");
        try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(tableSchema, cfg, ctx)) {
            Iterator<Object[]> rows = tableDataProvider.rows();

            long start = System.nanoTime();
            for (; ; ) {
                Table.TableBufferRoot table = writer.tableBufferRoot();
                for (int i = 0; i < batchSize; i++) {
                    if (!rows.hasNext()) {
                        break;
                    }
                    table.addRow(rows.next());
                }
                LOG.info("Table bytes used: {}", table.bytesUsed());
                // Complete the table; adding rows is no longer permitted.
                table.complete();

                // Write the table data to the server
                CompletableFuture<Integer> future = writer.writeNext();
                long fStart = System.nanoTime();
                future.whenComplete((r, t) -> {
                    long costMs = (System.nanoTime() - fStart) / 1000000;
                    if (t != null) {
                        LOG.error("Error writing data, time cost: {}ms", costMs, t);
                    } else {
                        LOG.info("Wrote rows: {}, time cost: {}ms", r, costMs);
                    }
                });

                if (!rows.hasNext()) {
                    break;
                }
            }

            writer.completed();

            LOG.info("Completed writing data, time cost: {}s", (System.nanoTime() - start) / 1000000000);
        } finally {
            tableDataProvider.close();
        }

        greptimeDB.shutdownGracefully();
    }
}
