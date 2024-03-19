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
 * @author jiachun.fjc
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

        // write data into stream
        writer.write(cpus);
        writer.write(memories);

        // delete the first 5 rows
        writer.write(cpus.subList(0, 5), WriteOp.Delete);

        // complete the stream
        CompletableFuture<WriteOk> future = writer.completed();

        WriteOk result = future.get();

        LOG.info("Write result: {}", result);

        // Shutdown the client when application exits.
        greptimeDB.shutdownGracefully();
    }
}
