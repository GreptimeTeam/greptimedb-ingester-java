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

import io.greptime.models.WriteOk;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to use the high-level API to write data to the database using stream.
 * It shows how to define the schema for metrics tables, write data to the stream, and get the write result.
 * It also shows how to delete data from the stream using the `WriteOp.Delete`.
 */
public class HighLevelApiStreamWriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(HighLevelApiStreamWriteQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        List<Cpu> cpus = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Cpu c = new Cpu();
            c.setHost("127.0.0." + i);
            c.setTs(System.currentTimeMillis());
            c.setCpuUser(i + 0.1);
            c.setCpuSys(i + 0.12);
            cpus.add(c);
        }

        List<Memory> memories = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Memory m = new Memory();
            m.setHost("127.0.0." + i);
            m.setTs(System.currentTimeMillis() / 1000);
            m.setMemUsage(i + 0.2);
            memories.add(m);
        }

        StreamWriter<List<?>, WriteOk> writer = greptimeDB.objectsStreamWriter();

        // Write data to the stream. The data will be immediately flushed to the network.
        // This allows for efficient, low-latency data transmission to the database.
        // Since this is client streaming, we cannot get the write result immediately.
        // After writing all data, we can call `completed()` to finalize the stream and get the result.
        writer.write(cpus);
        writer.write(memories);

        // Write a delete request to the stream to remove the first 5 rows from the cpuMetric table
        // This demonstrates how to selectively delete data using the `WriteOp.Delete`
        writer.write(cpus.subList(0, 5), WriteOp.Delete);

        // Completes the stream, and the stream will be closed.
        CompletableFuture<WriteOk> future = writer.completed();

        // Now we can get the write result.
        WriteOk result = future.get();

        LOG.info("Write result: {}", result);

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
