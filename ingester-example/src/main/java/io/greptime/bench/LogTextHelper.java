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

import java.util.concurrent.ThreadLocalRandom;

/**
 * Helper class for generating log messages.
 */
public class LogTextHelper {

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

    public static String randomLogLevel(ThreadLocalRandom random) {
        return LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
    }

    public static String generate2kText(ThreadLocalRandom random, long logTs) {
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
