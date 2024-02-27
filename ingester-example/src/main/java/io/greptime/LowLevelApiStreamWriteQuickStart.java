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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author jiachun.fjc
 */
public class LowLevelApiStreamWriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(LowLevelApiStreamWriteQuickStart.class);

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

        StreamWriter<Table, WriteOk> writer = greptimeDB.streamWriter();

        // write data into stream
        writer.write(cpuMetric);
        writer.write(memMetric);

        // delete the first 5 rows
        writer.write(cpuMetric.subRange(0, 5), WriteOp.Delete);

        // complete the stream
        CompletableFuture<WriteOk> future = writer.completed();

        WriteOk result = future.get();

        LOG.info("Write result: {}", result);

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
