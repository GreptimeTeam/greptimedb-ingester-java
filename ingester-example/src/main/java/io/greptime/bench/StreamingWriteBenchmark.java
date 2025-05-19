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

import io.greptime.GreptimeDB;
import io.greptime.StreamWriter;
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
 * - db_endpoint: the endpoint of the GreptimeDB server
 * - db_name: the name of the database
 * - batch_size_per_request: the batch size per request
 * - zstd_compression: whether to use zstd compression
 * - max_points_per_second: the max number of points that can be written per second, exceeding which may cause blockage
 */
public class StreamingWriteBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingWriteBenchmark.class);

    public static void main(String[] args) throws Exception {
        String endpoint = SystemPropertyUtil.get("db_endpoint", "127.0.0.1:4001");
        String dbName = SystemPropertyUtil.get("db_name", "public");
        boolean zstdCompression = SystemPropertyUtil.getBool("zstd_compression", true);
        int batchSize = SystemPropertyUtil.getInt("batch_size_per_request", 64 * 1024);
        int maxPointsPerSecond = SystemPropertyUtil.getInt("max_points_per_second", Integer.MAX_VALUE);
        LOG.info("Connect to db: {}, endpoint: {}", dbName, endpoint);
        LOG.info("Using zstd compression: {}", zstdCompression);
        LOG.info("Batch size: {}", batchSize);
        LOG.info("Max points per second: {}", maxPointsPerSecond);

        // Start a metrics exporter
        MetricsExporter metricsExporter = new MetricsExporter(MetricsUtil.metricRegistry());
        metricsExporter.init(ExporterOptions.newDefault());

        GreptimeDB greptimeDB = DBConnector.connectTo(new String[] {endpoint}, dbName);

        Compression compression = zstdCompression ? Compression.Zstd : Compression.None;
        Context ctx = Context.newDefault().withCompression(compression);

        StreamWriter<Table, WriteOk> writer = greptimeDB.streamWriter(maxPointsPerSecond, ctx);

        TableDataProvider tableDataProvider =
                ServiceLoader.load(TableDataProvider.class).first();
        LOG.info("Table data provider: {}", tableDataProvider.getClass().getName());
        tableDataProvider.init();
        TableSchema tableSchema = tableDataProvider.tableSchema();
        Iterator<Object[]> rows = tableDataProvider.rows();

        LOG.info("Start writing data");
        long start = System.nanoTime();
        for (; ; ) {
            Table table = Table.from(tableSchema);
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
            writer.write(table);

            if (!rows.hasNext()) {
                break;
            }
        }

        // Completes the stream, and the stream will be closed.
        CompletableFuture<WriteOk> future = writer.completed();

        // Now we can get the write result.
        WriteOk result = future.get();

        LOG.info("Completed writing data: {}, time cost: {}s", result, (System.nanoTime() - start) / 1000000000);

        greptimeDB.shutdownGracefully();
        tableDataProvider.close();
        metricsExporter.shutdownGracefully();
    }
}
