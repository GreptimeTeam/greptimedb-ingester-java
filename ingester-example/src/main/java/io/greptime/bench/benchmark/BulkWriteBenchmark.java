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

package io.greptime.bench.benchmark;

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.bench.DBConnector;
import io.greptime.bench.TableDataProvider;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.ServiceLoader;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.metrics.ExporterOptions;
import io.greptime.metrics.MetricsExporter;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkWriteBenchmark is a benchmark for the bulk write API of GreptimeDB.
 *
 * Env:
 * - zstd_compression: whether to use zstd compression
 * - batch_size_per_request: the batch size per request
 * - max_requests_in_flight: the max number of requests in flight
 * <p>
 * <b>IMPORTANT:</b> Unlike the standard write method,
 * this bulk writing stream API requires the target table to exist beforehand. It will
 * NOT automatically create the table if it does not exist. Please ensure table creation
 * before starting a bulk write operation.
 */
public class BulkWriteBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", false);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 64 * 1024);
        int maxRequestsInFlight = SystemPropertyUtil.getInt("max_requests_in_flight", 4);

        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);
        LOG.info("Max requests in flight: {}", maxRequestsInFlight);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();
        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(4 * 1024 * 1024 * 1024L)
                .timeoutMsPerMessage(60000)
                .maxRequestsInFlight(maxRequestsInFlight)
                .build();

        TableDataProvider tableDataProvider =
                ServiceLoader.load(TableDataProvider.class).first();
        LOG.info("Table data provider: {}", tableDataProvider.getClass().getName());
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();
        AtomicLong totalRowsWritten = new AtomicLong(0);

        // Before writing data, ensure the table exists, bulk write API does not create tables.
        ensureTableExists(greptimeDB, tableSchema, tableDataProvider, ctx);

        LOG.info("Start writing data");
        try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(tableSchema, cfg, ctx)) {
            Iterator<Object[]> rows = tableDataProvider.rows();

            long start = System.nanoTime();
            do {
                Table.TableBufferRoot table = writer.tableBufferRoot(1024);
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
                        return;
                    }

                    long totalRows = totalRowsWritten.addAndGet(r);
                    long totalElapsedSec = (System.nanoTime() - start) / 1000000000;
                    long writeRatePerSecond = totalElapsedSec > 0 ? totalRows / totalElapsedSec : 0;
                    LOG.info(
                            "Wrote rows: {}, time cost: {}ms, total rows: {}, total elapsed: {}s, write rate: {} rows/sec",
                            r,
                            costMs,
                            totalRows,
                            totalElapsedSec,
                            writeRatePerSecond);
                });
            } while (rows.hasNext());

            writer.completed();

            LOG.info("Completed writing data, time cost: {}s", (System.nanoTime() - start) / 1000000000);
        } finally {
            tableDataProvider.close();
        }

        greptimeDB.shutdownGracefully();
        metricsExporter.shutdownGracefully();
    }

    /**
     * Ensures that the table exists in the database.
     *
     * @param greptimeDB the GreptimeDB instance
     * @param tableSchema the schema of the table to ensure
     * @param ctx the context for the operation
     */
    private static void ensureTableExists(
            GreptimeDB greptimeDB, TableSchema tableSchema, TableDataProvider tableDataProvider, Context ctx) {
        Table initTable = Table.from(tableSchema);
        Iterator<Object[]> rows = tableDataProvider.rows();
        if (!rows.hasNext()) {
            throw new IllegalStateException("No rows available in table data provider");
        }
        // Add an initial row to the table to get the table schema.
        initTable.addRow(rows.next());
        try {
            // Write an initial row to ensure the table exists.
            greptimeDB
                    .write(Collections.singletonList(initTable), WriteOp.Insert, ctx)
                    .get();
            // Delete the initial row to leave the table empty.
            greptimeDB
                    .write(Collections.singletonList(initTable), WriteOp.Delete, ctx)
                    .get();
            LOG.info("Table ensured for benchmark: {}", tableSchema.getTableName());
        } catch (Exception e) {
            LOG.error("Table creation may have been skipped if it already exists: {}", e.getMessage());
        }
    }
}
