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

import io.greptime.models.DataType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to use the low-level API to write data to the database using stream.
 * It shows how to define the schema for metrics tables, write data to the stream, and get the write result.
 * It also shows how to delete data from the stream using the WriteOp.Delete.
 */
public class LowLevelApiStreamWriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(LowLevelApiStreamWriteQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        // Define the schema for metrics tables.
        // The schema is immutable and can be safely reused across multiple operations.
        // It is recommended to use snake_case for column names.
        TableSchema cpuMetricSchema = TableSchema.newBuilder("cpu_metric")
                .addTag("host", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("cpu_user", DataType.Float64)
                .addField("cpu_sys", DataType.Float64)
                .build();

        TableSchema memMetricSchema = TableSchema.newBuilder("mem_metric")
                .addTag("host", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("mem_usage", DataType.Float64)
                .build();

        // Tables are not reusable - a new instance must be created for each write operation.
        // However, we can add multiple rows to a single table before writing it,
        // which is more efficient than writing rows individually.
        Table cpuMetric = Table.from(cpuMetricSchema);
        Table memMetric = Table.from(memMetricSchema);

        for (int i = 0; i < 10; i++) {
            String host = "127.0.0." + i;
            long ts = System.currentTimeMillis();
            double cpuUser = i + 0.1;
            double cpuSys = i + 0.12;
            cpuMetric.addRow(host, ts, cpuUser, cpuSys);
        }

        for (int i = 0; i < 10; i++) {
            String host = "127.0.0." + i;
            long ts = System.currentTimeMillis();
            double memUsage = i + 0.2;
            memMetric.addRow(host, ts, memUsage);
        }

        // Complete the table to make it immutable. If users forget to call this method,
        // it will still be called internally before the table data is written.
        cpuMetric.complete();
        memMetric.complete();

        Context ctx = Context.newDefault().withCompression(Compression.Zstd);
        StreamWriter<Table, WriteOk> writer = greptimeDB.streamWriter(100000, ctx);

        // Write table data to the stream. The data will be immediately flushed to the network.
        // This allows for efficient, low-latency data transmission to the database.
        // Since this is client streaming, we cannot get the write result immediately.
        // After writing all data, we can call `completed()` to finalize the stream and get the result.
        writer.write(cpuMetric);
        writer.write(memMetric);

        // Write a delete request to the stream to remove the first 5 rows from the cpuMetric table
        // This demonstrates how to selectively delete data using the WriteOp.Delete
        writer.write(cpuMetric.subRange(0, 5), WriteOp.Delete);

        // Completes the stream, and the stream will be closed.
        CompletableFuture<WriteOk> future = writer.completed();

        // Now we can get the write result
        WriteOk result = future.get();

        LOG.info("Write result: {}", result);

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
