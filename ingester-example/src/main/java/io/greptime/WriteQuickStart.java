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
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author jiachun.fjc
 */
public class WriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(WriteQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        TableSchema cpuMetricSchema = TableSchema.newBuilder("cpu_metric") //
                .addTag("host", DataType.String) //
                .addTimestamp("ts", DataType.TimestampMillisecond) //
                .addField("cpu_user", DataType.Float64) //
                .addField("cpu_sys", DataType.Float64) //
                .build();

        TableSchema memMetricSchema = TableSchema.newBuilder("mem_metric") //
                .addTag("host", DataType.String) //
                .addTimestamp("ts", DataType.TimestampMillisecond) //
                .addField("mem_usage", DataType.Float64) //
                .build();

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

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.write(cpuMetric, memMetric);

        Result<WriteOk, Err> result = future.get();

        Result<Integer, String> simpleResult = result //
                .map(WriteOk::getSuccess) //
                .mapErr(err -> err.getError().getMessage());
        if (simpleResult.isOk()) {
            LOG.info("Write success: {}", simpleResult.getOk());
        } else {
            LOG.error("Failed to write: {}", simpleResult.getErr());
        }

        List<Table> delete_objs = Arrays.asList(cpuMetric.subRange(0, 5), memMetric.subRange(0, 5));
        Result<WriteOk, Err> deletes = greptimeDB.write(delete_objs, WriteOp.Delete).get();

        if (deletes.isOk()) {
            LOG.info("Delete result: {}", result.getOk());
        } else {
            LOG.error("Failed to delete: {}", result.getErr());
        }

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
