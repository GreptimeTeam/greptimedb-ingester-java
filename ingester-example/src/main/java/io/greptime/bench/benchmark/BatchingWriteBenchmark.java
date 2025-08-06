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

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.bench.BenchmarkResultPrinter;
import io.greptime.bench.DBConnector;
import io.greptime.bench.TableDataProvider;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.ServiceLoader;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.metrics.ExporterOptions;
import io.greptime.metrics.MetricsExporter;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchingWriteBenchmark is a benchmark for the batching write API of GreptimeDB.
 *
 * Env:
 * - zstd_compression: whether to use zstd compression
 * - batch_size_per_request: the batch size per request
 * - concurrency: the number of concurrent writers
 */
public class BatchingWriteBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(BatchingWriteBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", false);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 64 * 1024);
        int concurrency = SystemPropertyUtil.getInt("concurrency", 4);

        BenchmarkResultPrinter.printBenchmarkHeader(LOG, "Batching");
        BenchmarkResultPrinter.printConfiguration(LOG, "Batching", zstdCompression, batchSize, concurrency);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();

        Semaphore semaphore = new Semaphore(concurrency);
        TableDataProvider tableDataProvider =
                ServiceLoader.load(TableDataProvider.class).first();
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();
        AtomicLong totalRowsWritten = new AtomicLong(0);
        AtomicLong batchCounter = new AtomicLong(0);

        BenchmarkResultPrinter.printBenchmarkStart(
                LOG, "Batching", tableDataProvider, tableSchema, batchSize, concurrency);

        try {
            Iterator<Object[]> rows = tableDataProvider.rows();

            long benchmarkStart = System.nanoTime();
            do {
                Table table = Table.from(tableSchema);
                for (int j = 0; j < batchSize; j++) {
                    if (!rows.hasNext()) {
                        break;
                    }
                    table.addRow(rows.next());
                }

                // Complete the table; adding rows is no longer permitted.
                table.complete();

                semaphore.acquire();

                // Write the table data to the server
                CompletableFuture<Result<WriteOk, Err>> future =
                        greptimeDB.write(Collections.singletonList(table), WriteOp.Insert, ctx);
                future.whenComplete((result, error) -> {
                    semaphore.release();

                    if (error != null) {
                        LOG.error("Error writing data", error);
                        return;
                    }

                    int numRows = result.mapOr(0, writeOk -> writeOk.getSuccess());
                    long totalRows = totalRowsWritten.addAndGet(numRows);
                    long batch = batchCounter.incrementAndGet();
                    long totalElapsedMs = (System.nanoTime() - benchmarkStart) / 1000000;
                    long writeRatePerSecond = totalElapsedMs > 0 ? (totalRows * 1000) / totalElapsedMs : 0;
                    BenchmarkResultPrinter.printBatchProgress(LOG, batch, totalRows, writeRatePerSecond);
                });
            } while (rows.hasNext());

            // Wait for all the requests to complete
            semaphore.acquire(concurrency);

            BenchmarkResultPrinter.printCompletionMessages(LOG, "Batching");

            long totalDurationMs = (System.nanoTime() - benchmarkStart) / 1000000;
            long finalRowCount = totalRowsWritten.get();
            long finalThroughput = totalDurationMs > 0 ? (finalRowCount * 1000) / totalDurationMs : 0;

            BenchmarkResultPrinter.printBenchmarkSummary(
                    LOG, tableDataProvider, finalRowCount, totalDurationMs, finalThroughput);

        } finally {
            tableDataProvider.close();
            greptimeDB.shutdownGracefully();
            metricsExporter.shutdownGracefully();
        }
    }
}
