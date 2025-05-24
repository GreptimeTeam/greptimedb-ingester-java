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

package io.greptime.quickstart.write;

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.quickstart.TestConnector;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to use the low-level API to write data to the database.
 * It shows how to define the schema for metrics tables, write data to the table, and get the write result.
 * It also shows how to delete data from the table using the `WriteOp.Delete`.
 */
public class LowLevelApiWriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(LowLevelApiWriteQuickStart.class);

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
            // Add a row to the `cpu_metric` table.
            // The order of the values must match the schema definition.
            cpuMetric.addRow(host, ts, cpuUser, cpuSys);
        }

        for (int i = 0; i < 10; i++) {
            String host = "127.0.0." + i;
            long ts = System.currentTimeMillis();
            double memUsage = i + 0.2;
            // Add a row to the `mem_metric` table.
            // The order of the values must match the schema definition.
            memMetric.addRow(host, ts, memUsage);
        }

        // Complete the table to make it immutable. If users forget to call this method,
        // it will still be called internally before the table data is written.
        cpuMetric.complete();
        memMetric.complete();

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a CompletableFuture object. If you want to immediately obtain
        // the result, you can call `future.get()`, which will block until the operation completes.
        // For production environments, consider using non-blocking approaches with callbacks or
        // the CompletableFuture API.
        CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.write(cpuMetric, memMetric);

        // Now we can get the write result.
        Result<WriteOk, Err> result = future.get();

        // The Result object holds either a success value (WriteOk) or an error (Err).
        // We can transform these values using the `map` method for success cases and `mapErr` for error cases.
        // In this example, we extract just the success count from `WriteOk` and the error message from `Err`
        // to create a simplified result that's easier to work with in our application logic.
        Result<Integer, String> simpleResult =
                result.map(WriteOk::getSuccess).mapErr(err -> err.getError().getMessage());
        if (simpleResult.isOk()) {
            LOG.info("Write success: {}", simpleResult.getOk());
        } else {
            LOG.error("Failed to write: {}", simpleResult.getErr());
        }

        List<Table> delete_objs = Arrays.asList(cpuMetric.subRange(0, 5), memMetric.subRange(0, 5));
        // We can also delete data from the table using the `WriteOp.Delete`.
        Result<WriteOk, Err> deletes =
                greptimeDB.write(delete_objs, WriteOp.Delete).get();

        if (deletes.isOk()) {
            LOG.info("Delete result: {}", result.getOk());
        } else {
            LOG.error("Failed to delete: {}", result.getErr());
        }

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
