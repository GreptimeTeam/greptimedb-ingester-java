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
import io.grpc.netty.shaded.io.netty.util.internal.ThreadLocalRandom;
import java.util.Iterator;

@SPI(
        name = "random_table_data_provider",
        priority = 1 /* newer implementation can use higher priority to override the old one */)
public class RandomTableDataProvider implements TableDataProvider {

    private final TableSchema tableSchema;
    private final long rowCount;

    {
        tableSchema = TableSchema.newBuilder("my_bench_table")
                .addTimestamp("log_ts", DataType.TimestampMillisecond)
                .addTag("business_name", DataType.String)
                .addTag("app_name", DataType.String)
                .addTag("host_name", DataType.String)
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
        // Default is 1 billion rows if not specified
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
                String logMessage = generate2kText(random, logTs);
                String logLevel = LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
                String logName = "log_name_" + random.nextInt(100);
                String uri = "http://example.com/path_" + random.nextInt(1000);
                String traceId = "trace_" + random.nextLong(1000000);
                String spanId = "span_" + random.nextLong(1000000);
                String errno = "errno_" + random.nextInt(1000);
                String traceFlags = "trace_flags_" + random.nextInt(1000);
                String traceState = "trace_state_" + random.nextInt(1000);
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

    private static final String[] LOG_LEVELS = {"INFO", "WARN", "ERROR", "DEBUG"};

    // Log templates as static constants to avoid recreating them for each call
    private static final String[] LOG_TEMPLATES = {
        "INFO: Request processed successfully. RequestID: %s, Duration: %dms, UserID: %s",
        "WARN: Slow query detected. QueryID: %s, Duration: %dms, SQL: %s",
        "ERROR: Failed to connect to database. Attempt: %d, Error: %s, Host: %s",
        "DEBUG: Cache hit ratio: %d%%, Keys: %d, Misses: %d, Memory usage: %dMB",
        "INFO: User authentication successful. UserID: %s, IP: %s, LoginTime: %s"
    };

    // Stack trace frames as static constants
    private static final String[] STACK_FRAMES = {
        "at io.greptime.service.DatabaseConnector.connect(DatabaseConnector.java:142)",
        "at io.greptime.service.QueryExecutor.execute(QueryExecutor.java:85)",
        "at io.greptime.api.RequestHandler.processQuery(RequestHandler.java:213)",
        "at io.greptime.server.GrpcService.handleRequest(GrpcService.java:178)",
        "at io.greptime.server.HttpEndpoint.doPost(HttpEndpoint.java:95)",
        "at javax.servlet.http.HttpServlet.service(HttpServlet.java:707)",
        "at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:227)"
    };

    // Context keys as static constants
    private static final String[] CONTEXT_KEYS = {
        "client_version", "server_version", "cluster_id", "node_id", "region", "datacenter"
    };

    // Pre-computed stack trace and context strings to avoid concatenation in hot path
    private static final String STACK_TRACE_PREFIX = "\nStack trace:\n";
    private static final String CONTEXT_PREFIX = "\nAdditional context: ";
    private static final String CONTEXT_SEPARATOR = ", ";
    private static final String CONTEXT_EQUALS = "=";

    static String generate2kText(ThreadLocalRandom random, long logTs) {
        StringBuilder buf = new StringBuilder(2000);

        // Choose one of several log templates to make it more realistic
        String template = LOG_TEMPLATES[random.nextInt(LOG_TEMPLATES.length)];

        // Format the template with the prepared values
        String formattedLog;
        if (template.startsWith("INFO: Request")) {
            String requestId = "req_" + random.nextLong(1000000);
            int duration = random.nextInt(1, 5000);
            String userId = "user_" + random.nextInt(10000);
            formattedLog = String.format(template, requestId, duration, userId);
        } else if (template.startsWith("WARN:")) {
            String queryId = "query_" + random.nextInt(50000);
            int duration = random.nextInt(1, 5000);
            formattedLog = String.format(template, queryId, duration, "SELECT * FROM table_" + random.nextInt(100));
        } else if (template.startsWith("ERROR:")) {
            formattedLog = String.format(template, random.nextInt(5), "Connection refused", "db-" + random.nextInt(10));
        } else if (template.startsWith("DEBUG:")) {
            int count = random.nextInt(10000);
            int percentage = random.nextInt(100);
            formattedLog = String.format(template, percentage, count, random.nextInt(1000), random.nextInt(512));
        } else { // Second INFO template
            String userId = "user_" + random.nextInt(10000);
            formattedLog = String.format(
                    template, userId, "192.168." + random.nextInt(256) + "." + random.nextInt(256), logTs);
        }

        buf.append(formattedLog);

        // Target length between 1600-1900 characters
        int targetLength = random.nextInt(1600, 1900);

        // Add stack trace for error logs to reach desired size
        if (formattedLog.startsWith("ERROR")) {
            buf.append(STACK_TRACE_PREFIX);

            // Add stack frames until we reach target length
            int frameIndex = 0;
            int framesCount = STACK_FRAMES.length;

            while (buf.length() < targetLength && frameIndex < 100) { // Limit iterations
                buf.append(STACK_FRAMES[frameIndex % framesCount]).append('\n');
                frameIndex++;
            }
        } else {
            // For non-error logs, pad with additional context information
            buf.append(CONTEXT_PREFIX);

            // Add key-value pairs until we reach desired length
            int keyIndex = 0;
            int keysCount = CONTEXT_KEYS.length;

            while (buf.length() < targetLength && keyIndex < 100) { // Limit iterations
                buf.append(CONTEXT_KEYS[keyIndex % keysCount])
                        .append(CONTEXT_EQUALS)
                        .append(random.nextInt(1000))
                        .append(CONTEXT_SEPARATOR);
                keyIndex++;
            }
        }

        return buf.toString();
    }
}
