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

import io.greptime.metric.Cpu;
import io.greptime.GreptimeDB;
import io.greptime.metric.Memory;
import io.greptime.WriteOp;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import io.greptime.quickstart.TestConnector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to use the high-level API to write data to the database.
 * It shows how to define the schema for metrics tables, write data to the table, and get the write result.
 * It also shows how to delete data from the table using the `WriteOp.Delete`.
 */
public class HighLevelApiWriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(HighLevelApiWriteQuickStart.class);

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
            m.setTs(System.currentTimeMillis());
            m.setMemUsage(i + 0.2);
            memories.add(m);
        }

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a CompletableFuture object. If you want to immediately obtain
        // the result, you can call `future.get()`, which will block until the operation completes.
        // For production environments, consider using non-blocking approaches with callbacks or
        // the CompletableFuture API.
        CompletableFuture<Result<WriteOk, Err>> puts = greptimeDB.writeObjects(cpus, memories);

        // Now we can get the write result.
        Result<WriteOk, Err> result = puts.get();

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

        List<List<?>> deletePojoObjects = Arrays.asList(cpus.subList(0, 5), memories.subList(0, 5));
        // We can also delete data from the table using the `WriteOp.Delete`.
        Result<WriteOk, Err> deletes =
                greptimeDB.writeObjects(deletePojoObjects, WriteOp.Delete).get();

        if (deletes.isOk()) {
            LOG.info("Delete result: {}", result.getOk());
        } else {
            LOG.error("Failed to delete: {}", result.getErr());
        }

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
