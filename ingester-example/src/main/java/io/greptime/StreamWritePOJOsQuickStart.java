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
import io.greptime.options.GreptimeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author jiachun.fjc
 */
public class StreamWritePOJOsQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(StreamWritePOJOsQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // GreptimeDB has a default database named "public", we can use it as the test database
        String database = "public";
        // By default, GreptimeDB listens on port 4001 using the gRPC protocol.
        // We can provide multiple endpoints that point to the same GreptimeDB cluster.
        // The client will make calls to these endpoints based on a load balancing strategy.
        String[] endpoints = {"127.0.0.1:4001"};
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database) //
                .build();

        GreptimeDB greptimeDB = GreptimeDB.create(opts);

        List<MyMetric1> myMetric1s = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MyMetric1 m = new MyMetric1();
            m.setTag1("tag_value_1_" + i);
            m.setTag2("tag_value_2_" + i);
            m.setTag3("tag_value_3_" + i);
            m.setTs(System.currentTimeMillis());
            m.setField1("field_value_1_" + i);
            m.setField2(i);
            m.setField3(new BigDecimal(i));
            m.setField4(i);

            myMetric1s.add(m);
        }

        List<MyMetric2> myMetric2s = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MyMetric2 m = new MyMetric2();
            m.setTag1("tag_value_1_" + i);
            m.setTag2("tag_value_2_" + i);
            m.setTs(System.currentTimeMillis() / 1000);
            m.setField1(Calendar.getInstance().getTime());
            m.setField2(i);

            myMetric2s.add(m);
        }

        StreamWriter<List<?>, WriteOk> writer = greptimeDB.streamWriterPOJOs();

        // write data into stream
        writer.write(myMetric1s);
        writer.write(myMetric2s);

        // delete the first 5 rows
        writer.write(myMetric1s.subList(0, 5), WriteOp.Delete);

        // complete the stream
        CompletableFuture<WriteOk> future = writer.completed();

        WriteOk result = future.get();

        LOG.info("Write result: {}", result);
    }
}
