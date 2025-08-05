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
import io.greptime.StreamWriter;
import io.greptime.bench.BenchmarkResultPrinter;
import io.greptime.bench.DBConnector;
import io.greptime.bench.TableDataProvider;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.ServiceLoader;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.metrics.ExporterOptions;
import io.greptime.metrics.MetricsExporter;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingWriteBenchmark is a benchmark for the streaming write API of GreptimeDB.
 *
 * Env:
 * - batch_size_per_request: the batch size per request
 * - zstd_compression: whether to use zstd compression
 * - max_points_per_second: the max number of points that can be written per second, exceeding which may cause blockage
 */
public class StreamingWriteBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingWriteBenchmark.class);

    public static void main(String[] args) throws Exception {
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", false);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 64 * 1024);
        int maxPointsPerSecond = SystemPropertyUtil.getInt("max_points_per_second", Integer.MAX_VALUE);

        BenchmarkResultPrinter.printBenchmarkHeader(LOG, "Streaming");
        BenchmarkResultPrinter.printConfiguration(LOG, "Streaming", zstdCompression, batchSize, 0, maxPointsPerSecond);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connect();

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        StreamWriter<Table, WriteOk> writer = greptimeDB.streamWriter(maxPointsPerSecond, ctx);

        TableDataProvider tableDataProvider =
                ServiceLoader.load(TableDataProvider.class).first();
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();
        Iterator<Object[]> rows = tableDataProvider.rows();

        BenchmarkResultPrinter.printBenchmarkStart(
                LOG, "Streaming", tableDataProvider, tableSchema, batchSize, 0, maxPointsPerSecond);

        long benchmarkStart = System.nanoTime();
        long totalRowsWritten = 0;
        int batchCounter = 0;
        do {
            Table table = Table.from(tableSchema);
            for (int i = 0; i < batchSize; i++) {
                if (!rows.hasNext()) {
                    break;
                }
                table.addRow(rows.next());
            }
            int rowsInBatch = table.rowCount();
            totalRowsWritten += rowsInBatch;
            batchCounter++;

            long totalElapsedMs = (System.nanoTime() - benchmarkStart) / 1000000;
            long writeRatePerSecond = totalElapsedMs > 0 ? (totalRowsWritten * 1000) / totalElapsedMs : 0;
            BenchmarkResultPrinter.printBatchProgress(LOG, batchCounter, totalRowsWritten, writeRatePerSecond);

            // Complete the table; adding rows is no longer permitted.
            table.complete();
            // Write the table data to the server
            writer.write(table);

        } while (rows.hasNext());

        // Completes the stream, and the stream will be closed.
        CompletableFuture<WriteOk> future = writer.completed();

        // Now we can get the writing result.
        future.get();

        BenchmarkResultPrinter.printCompletionMessages(LOG, "Streaming");

        long totalDurationMs = (System.nanoTime() - benchmarkStart) / 1000000;
        long finalThroughput = totalDurationMs > 0 ? (totalRowsWritten * 1000) / totalDurationMs : 0;

        BenchmarkResultPrinter.printFinalResults(LOG, totalRowsWritten, totalDurationMs, finalThroughput);
        BenchmarkResultPrinter.printProviderResults(
                LOG, tableDataProvider, totalRowsWritten, totalDurationMs, finalThroughput);
        BenchmarkResultPrinter.printBenchmarkSummary(
                LOG, tableDataProvider, totalRowsWritten, totalDurationMs, finalThroughput);

        greptimeDB.shutdownGracefully();
        tableDataProvider.close();
        metricsExporter.shutdownGracefully();
    }
}
