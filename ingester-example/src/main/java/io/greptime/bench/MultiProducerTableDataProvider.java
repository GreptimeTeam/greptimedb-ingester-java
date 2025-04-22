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

package io.greptime.bench;

import com.codahale.metrics.Timer;
import io.greptime.common.SPI;
import io.greptime.common.util.ExecutorServiceHelper;
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.common.util.ThreadPoolUtil;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

@SPI(
        name = "multi_producer_table_data_provider",
        priority = 10 /* newer implementation can use higher priority to override the old one */)
public class MultiProducerTableDataProvider extends RandomTableDataProvider {

    private final int producerCount;
    private final long rowCount;
    private final ExecutorService executorService;
    private final BlockingQueue<Object[]> buffer = new ArrayBlockingQueue<>(100000);

    {
        this.producerCount = SystemPropertyUtil.getInt("multi_producer_table_data_provider.producer_count", 10);
        // Total number of rows to generate, configurable via system property
        this.rowCount = SystemPropertyUtil.getLong("table_data_provider.row_count", 10_000_000L);
        this.executorService = ThreadPoolUtil.newBuilder()
                .poolName("multi-producer-table-data-provider")
                .enableMetric(true)
                .coreThreads(this.producerCount)
                .maximumThreads(this.producerCount)
                .keepAliveSeconds(60L)
                .workQueue(new ArrayBlockingQueue<>(512))
                .threadFactory(new NamedThreadFactory("multi-producer-table-data-provider"))
                .rejectedHandler(new ThreadPoolExecutor.CallerRunsPolicy())
                .build();
    }

    @Override
    public void init() {
        AtomicLong rowIndex = new AtomicLong(0);
        for (int i = 0; i < producerCount; i++) {
            this.executorService.execute(() -> {
                while (rowIndex.getAndIncrement() < rowCount) {
                    Object[] row = nextRow();
                    try {
                        buffer.put(row);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    @Override
    public Iterator<Object[]> rows() {

        return new Iterator<Object[]>() {
            private long index = 0;

            @Override
            public boolean hasNext() {
                return index < rowCount;
            }

            @Override
            public Object[] next() {
                try {
                    Object[] row = buffer.take();
                    index++;
                    return row;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void close() throws Exception {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.executorService);
    }

    public Object[] nextRow() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Timer.Context timerContext =
                MetricsUtil.timer("multi_producer_table_data_provider.next_row").time();
        long logTs = System.currentTimeMillis();
        String businessName = "business_" + random.nextInt(10);
        String appName = "app_" + random.nextInt(100);
        String hostName = "host_" + random.nextInt(1000);
        String logMessage = LogTextHelper.generate2kText(random, logTs);
        String logLevel = LogTextHelper.randomLogLevel(random);
        String logName = "log_name_" + random.nextInt(100);
        String uri = "http://example.com/path_" + random.nextInt(1000);
        String traceId = "trace_" + random.nextLong(1000000);
        String spanId = "span_" + random.nextLong(1000000);
        String errno = "errno_" + random.nextInt(1000);
        String traceFlags = "trace_flags_" + random.nextInt(1000);
        String traceState = "trace_state_" + random.nextInt(1000);
        String podName = "pod_" + random.nextInt(1000);
        timerContext.stop();
        MetricsUtil.histogram("random_table_data_provider.log_message_length").update(logMessage.length());

        return new Object[] {
            logTs,
            businessName,
            appName,
            hostName,
            logMessage,
            logLevel,
            logName,
            uri,
            traceId,
            spanId,
            errno,
            traceFlags,
            traceState,
            podName
        };
    }
}
