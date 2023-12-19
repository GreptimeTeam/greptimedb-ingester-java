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

import io.greptime.models.Column;
import io.greptime.models.DataType;
import io.greptime.models.Database;
import io.greptime.models.Err;
import io.greptime.models.Metric;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import io.greptime.options.GreptimeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author jiachun.fjc
 */
public class QuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(QuickStart.class);

    public static void main(String[] args) throws Exception {
        String endpoint = "127.0.0.1:4001";
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoint) //
                .writeMaxRetries(1) //
                .routeTableRefreshPeriodSeconds(-1) //
                .build();

        GreptimeDB greptimeDB = new GreptimeDB();

        if (!greptimeDB.init(opts)) {
            throw new RuntimeException("Fail to start GreptimeDB client");
        }

        long now = System.currentTimeMillis();
        LOG.info("now = {}", now);

        // normal inset
        runInsert(greptimeDB, now);

        // streaming insert
        runInsertWithStream(greptimeDB, now);
    }

    @Database(name = "public")
    @Metric(name = "monitor")
    static class Monitor {
        @Column(name = "host", tag = true, dataType = DataType.String)
        String host;
        @Column(name = "ts", timestamp = true, dataType = DataType.TimestampMillisecond)
        long ts;
        @Column(name = "cpu", dataType = DataType.Float64)
        double cpu;
        @Column(name = "memory", dataType = DataType.Float64)
        double memory;
        @Column(name = "decimal_value", dataType = DataType.Decimal128)
        BigDecimal decimalValue;
    }

    @Database(name = "public")
    @Metric(name = "monitor_cpu")
    static class MonitorCpu {
        @Column(name = "host", tag = true, dataType = DataType.String)
        String host;
        @Column(name = "ts", timestamp = true, dataType = DataType.TimestampMillisecond)
        long ts;
        @Column(name = "cpu", dataType = DataType.Float64)
        double cpu;
    }

    private static void runInsert(GreptimeDB greptimeDB, long now) throws Exception {
        List<Monitor> monitors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Monitor monitor = new Monitor();
            monitor.host = "127.0.0." + i;
            monitor.ts = now + i;
            monitor.cpu = i;
            monitor.memory = i * 2;
            monitor.decimalValue = new BigDecimal("1111111111111111111.2333333" + i);
            monitors.add(monitor);
        }

        List<MonitorCpu> monitorCpus = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            MonitorCpu monitor = new MonitorCpu();
            monitor.host = "127.0.0." + i;
            monitor.ts = now + i;
            monitor.cpu = i;
            monitorCpus.add(monitor);
        }

        List<List<?>> pojos = new ArrayList<>();
        pojos.add(monitors);
        pojos.add(monitorCpus);

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.writePOJOs(pojos);

        Result<WriteOk, Err> result = future.get();

        if (result.isOk()) {
            LOG.info("Write result: {}", result.getOk());
        } else {
            LOG.error("Failed to write: {}", result.getErr());
        }
    }

    private static void runInsertWithStream(GreptimeDB greptimeDB, long now) throws Exception {
        StreamWriter<List<?>, WriteOk> streamWriter = greptimeDB.streamWriterPOJOs();

        List<Monitor> monitors = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Monitor monitor = new Monitor();
            monitor.host = "127.0.0." + i;
            monitor.ts = now + i;
            monitor.cpu = i;
            monitor.memory = i * 2;
            monitor.decimalValue = new BigDecimal("1111111111111111111.2333333" + i);
            monitors.add(monitor);
        }

        streamWriter.write(monitors);

        List<MonitorCpu> monitorCpus = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            MonitorCpu monitor = new MonitorCpu();
            monitor.host = "127.0.0." + i;
            monitor.ts = now + i;
            monitor.cpu = i;
            monitorCpus.add(monitor);
        }

        streamWriter.write(monitorCpus);

        CompletableFuture<WriteOk> future = streamWriter.completed();

        WriteOk result = future.get();

        LOG.info("Write result: {}", result);
    }
}
