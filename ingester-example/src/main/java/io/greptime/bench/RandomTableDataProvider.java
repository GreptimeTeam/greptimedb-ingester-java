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
import io.greptime.common.util.MetricsUtil;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

@SPI(
        name = "random_table_data_provider",
        priority = 10 /* newer implementation can use higher priority to override the old one */)
public class RandomTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;

    {
        tableSchema = TableSchema.newBuilder("my_bench_table")
                .addTimestamp("log_ts", DataType.TimestampMillisecond)
                .addField("business_name", DataType.String)
                .addField("app_name", DataType.String)
                .addField("host_name", DataType.String)
                .addField("log_message", DataType.String) // 2K
                .addField("log_level", DataType.String)
                .addField("log_name", DataType.String)
                .addField("uri", DataType.String)
                .addField("trace_id", DataType.String)
                .addField("span_id", DataType.String)
                .addField("errno", DataType.String)
                .addField("trace_flags", DataType.String)
                .addField("trace_state", DataType.String)
                .addField("pod_name", DataType.String)
                .build();
        // Total number of rows to generate, configurable via system property
        rowCount = SystemPropertyUtil.getLong("table_data_provider.row_count", 1_000_000L);
    }

    @Override
    public void init() {}

    @Override
    public TableSchema tableSchema() {
        return this.tableSchema;
    }

    @Override
    public Iterator<Object[]> rows() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return new Iterator<Object[]>() {

            private long index = 0;

            @Override
            public boolean hasNext() {
                return index < rowCount;
            }

            @Override
            public Object[] next() {
                index++;

                Timer.Context timerContext =
                        MetricsUtil.timer("random_table_data_provider.next_row").time();
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
                String traceFlags;
                int flags = random.nextInt(1000);
                if (flags % 2 == 0) {
                    traceFlags = "trace_flags_" + flags;
                } else {
                    traceFlags = null;
                }
                int state = random.nextInt(1000);
                String traceState;
                if (state % 3 == 0) {
                    traceState = "trace_state_" + state;
                } else {
                    traceState = null;
                }
                String podName = "pod_" + random.nextInt(1000);
                timerContext.stop();
                MetricsUtil.histogram("random_table_data_provider.log_message_length")
                        .update(logMessage.length());

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
        };
    }

    @Override
    public void close() throws Exception {}

    @Override
    public long rowCount() {
        return rowCount;
    }
}
