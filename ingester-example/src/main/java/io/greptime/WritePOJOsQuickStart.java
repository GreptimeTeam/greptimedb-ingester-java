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

import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author jiachun.fjc
 */
public class WritePOJOsQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(WritePOJOsQuickStart.class);

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
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<WriteOk, Err>> puts = greptimeDB.writePOJOs(cpus, memories);

        Result<WriteOk, Err> result = puts.get();

        if (result.isOk()) {
            LOG.info("Write result: {}", result.getOk());
        } else {
            LOG.error("Failed to write: {}", result.getErr());
        }

        List<List<?>> delete_pojos = Arrays.asList(cpus.subList(0, 5), memories.subList(0, 5));
        Result<WriteOk, Err> deletes = greptimeDB.writePOJOs(delete_pojos, WriteOp.Delete).get();

        if (deletes.isOk()) {
            LOG.info("Delete result: {}", result.getOk());
        } else {
            LOG.error("Failed to delete: {}", result.getErr());
        }

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
