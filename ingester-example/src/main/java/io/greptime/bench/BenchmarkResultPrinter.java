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

import io.greptime.common.util.Cpus;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.models.TableSchema;
import org.slf4j.Logger;

/**
 * Utility class for printing benchmark results in a consistent format.
 */
public class BenchmarkResultPrinter {

    public static void printBenchmarkHeader(Logger log, String apiType) {
        log.info("=== GreptimeDB {} API Log Benchmark ===", apiType);
        log.info("Synthetic log data generation and {} API ingestion performance test", apiType.toLowerCase());
        log.info("");
    }

    public static void printConfiguration(
            Logger log, String apiType, boolean zstdCompression, int batchSize, int parallelismOrConcurrency) {
        printConfiguration(log, apiType, zstdCompression, batchSize, parallelismOrConcurrency, null);
    }

    public static void printConfiguration(
            Logger log,
            String apiType,
            boolean zstdCompression,
            int batchSize,
            int parallelismOrConcurrency,
            Integer maxPointsPerSecond) {
        log.info("=== {} API Benchmark Configuration ===", apiType);

        String endpointsStr = SystemPropertyUtil.get("db.endpoints");
        if (endpointsStr == null) {
            endpointsStr = "localhost:4001";
        }
        String database = SystemPropertyUtil.get("db.database");
        if (database == null) {
            database = "public";
        }

        log.info("Endpoint: {}", endpointsStr);
        log.info("Database: {}", database);
        log.info("Batch size: {}", batchSize);

        if (maxPointsPerSecond != null) {
            log.info(
                    "Max points per second: {}",
                    maxPointsPerSecond == Integer.MAX_VALUE ? "unlimited" : maxPointsPerSecond);
        } else if (apiType.equals("Bulk")) {
            log.info("Parallelism: {}", parallelismOrConcurrency);
        } else {
            log.info("Concurrency: {}", parallelismOrConcurrency);
        }

        log.info("Compression: {}", (zstdCompression ? "zstd" : "none"));
        log.info("CPU cores: {}", Cpus.cpus());
        log.info("Build profile: release");
        log.info("");
    }

    public static void printBenchmarkStart(
            Logger log,
            String apiType,
            TableDataProvider provider,
            TableSchema schema,
            int batchSize,
            int parallelismOrConcurrency) {
        printBenchmarkStart(log, apiType, provider, schema, batchSize, parallelismOrConcurrency, null);
    }

    public static void printBenchmarkStart(
            Logger log,
            String apiType,
            TableDataProvider provider,
            TableSchema schema,
            int batchSize,
            int parallelismOrConcurrency,
            Integer maxPointsPerSecond) {
        log.info("=== Running {} API Log Data Benchmark ===", apiType);
        log.info("Setting up {} writer...", apiType.toLowerCase());
        log.info(
                "Starting {} API benchmark: {}",
                apiType.toLowerCase(),
                provider.getClass().getSimpleName());
        log.info(
                "Table: {} ({} columns)",
                schema.getTableName(),
                schema.getColumnNames().size());
        log.info("Target rows: {}", provider.rowCount());
        log.info("Batch size: {}", batchSize);

        if (maxPointsPerSecond != null) {
            log.info(
                    "Max points per second: {}",
                    maxPointsPerSecond == Integer.MAX_VALUE ? "unlimited" : maxPointsPerSecond);
        } else if (apiType.equals("Bulk")) {
            log.info("Parallelism: {}", parallelismOrConcurrency);
        } else {
            log.info("Concurrency: {}", parallelismOrConcurrency);
        }

        log.info("");
    }

    public static void printBatchProgress(Logger log, long batch, long totalRows, long writeRatePerSecond) {
        log.info("→ Batch {}: {} rows processed ({} rows/sec)", batch, totalRows, writeRatePerSecond);

        if (batch % 10 == 0) {
            log.info("Flushed {} responses (total {} affected rows)", batch, totalRows);
        }
    }

    public static void printCompletionMessages(Logger log, String apiType) {
        log.info("Finishing {} writer and waiting for all responses...", apiType.toLowerCase());
        log.info("All {} writes completed successfully", apiType.toLowerCase());
        log.info("Cleaning up data provider...");
        log.info("{} API benchmark completed successfully!", apiType);
    }

    public static void printFinalResults(Logger log, long totalRows, long durationMs, long throughput) {
        log.info("Final Result:");
        log.info("• Total rows: {}", totalRows);
        log.info("• Total batches: {}", (totalRows / 100000));
        log.info("• Duration: {}s", durationMs / 1000.0);
        log.info("• Throughput: {} rows/sec", throughput);
        log.info("");
    }

    public static void printProviderResults(
            Logger log, TableDataProvider provider, long totalRows, long durationMs, long throughput) {
        log.info("=== {} Benchmark Results ===", provider.getClass().getSimpleName());
        log.info("Table: {}", provider.tableSchema().getTableName());
        log.info("SUCCESS");
        log.info("Total rows: {}", totalRows);
        log.info("Duration: {}ms", durationMs);
        log.info("Throughput: {} rows/sec", throughput);
        log.info("");
    }

    public static void printBenchmarkSummary(
            Logger log, TableDataProvider provider, long totalRows, long durationMs, long throughput) {
        log.info("=== Benchmark Result ===");
        log.info("Fastest provider: {} ({} rows/sec)", provider.getClass().getSimpleName(), throughput);
        log.info("");
        log.info("Provider                          Rows Duration(ms)      Throughput     Status");
        log.info("--------------------------------------------------------------------------");
        log.info(String.format(
                "%-30s %8d %12d %12d r/s    SUCCESS",
                provider.getClass().getSimpleName(), totalRows, durationMs, throughput));
    }
}
