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

import io.greptime.BulkStreamWriter;
import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.quickstart.TestConnector;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to retry transient bulk write failures.
 *
 * <p>A failed bulk stream cannot be reused. Before retrying, the example closes the old writer, creates a new one,
 * rebuilds the Arrow buffer from retained Java rows, and then sends the batch again.
 *
 * <p>A retry provides at-least-once delivery. If the server committed a batch but its response was lost, replaying the
 * batch can write it more than once. Applications should choose keys and data semantics that tolerate replay.
 */
public class BulkWriteWithRetryQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteWithRetryQuickStart.class);

    private static final int COLUMN_BUFFER_SIZE = 1024;
    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_RETRY_DELAY_MILLIS = 2_000;
    private static final long MAX_RETRY_DELAY_MILLIS = 30_000;

    public static void main(String[] args) throws Exception {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        try {
            TableSchema schema = TableSchema.newBuilder("bulk_write_retry_demo")
                    .addTag("host", DataType.String)
                    .addTimestamp("ts", DataType.TimestampMillisecond)
                    .addField("cpu_usage", DataType.Float64)
                    .build();

            // Bulk write does not create tables automatically. Use the regular write API to create it first.
            Table tableToCreate = Table.from(schema);
            tableToCreate.addRow("bootstrap", System.currentTimeMillis(), 0.0);
            tableToCreate.complete();
            Result<WriteOk, Err> createResult = greptimeDB.write(tableToCreate).get();
            if (!createResult.isOk()) {
                Err err = createResult.getErr();
                throw new IllegalStateException(
                        "Failed to create the bulk write table, status code: "
                                + err.getCode()
                                + ", endpoint: "
                                + err.getErrTo(),
                        err.getError());
            }

            BulkWrite.Config config = BulkWrite.Config.newBuilder()
                    .allocatorInitReservation(0)
                    .allocatorMaxAllocation(1024 * 1024 * 1024L)
                    .timeoutMsPerMessage(60_000)
                    .maxRequestsInFlight(8)
                    .build();
            Context context = Context.newDefault().withCompression(Compression.None);

            try (RetryingBulkWriter writer = new RetryingBulkWriter(greptimeDB, schema, config, context)) {
                for (int batchIndex = 0; batchIndex < 10; batchIndex++) {
                    // Retain the batch outside TableBufferRoot. writeNext() clears the Arrow buffer, including on
                    // failure, so these rows are the source used to rebuild the buffer after reconnecting.
                    List<Object[]> rows = createBatch(batchIndex, 10_000);
                    int affectedRows = writer.writeBatch(rows);
                    LOG.info("Bulk write batch {} succeeded, affected rows: {}", batchIndex, affectedRows);
                }

                writer.completed();
            }
        } finally {
            greptimeDB.shutdownGracefully();
        }
    }

    private static List<Object[]> createBatch(int batchIndex, int rowCount) {
        List<Object[]> rows = new ArrayList<>(rowCount);
        long timestamp = System.currentTimeMillis();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            rows.add(new Object[] {
                "host-" + rowIndex, timestamp + batchIndex, random.nextDouble(0.0, 100.0),
            });
        }
        return rows;
    }

    private static final class RetryingBulkWriter implements AutoCloseable {

        private final GreptimeDB greptimeDB;
        private final TableSchema schema;
        private final BulkWrite.Config config;
        private final Context context;

        private BulkStreamWriter writer;
        private boolean completed;

        private RetryingBulkWriter(
                GreptimeDB greptimeDB, TableSchema schema, BulkWrite.Config config, Context context) {
            this.greptimeDB = greptimeDB;
            this.schema = schema;
            this.config = config;
            this.context = context;
        }

        private int writeBatch(List<Object[]> rows) throws Exception {
            if (this.completed) {
                throw new IllegalStateException("The bulk writer is already completed");
            }
            if (rows.isEmpty()) {
                return 0;
            }

            for (int retry = 0; ; retry++) {
                try {
                    BulkStreamWriter currentWriter = getOrCreateWriter();
                    Table.TableBufferRoot table = currentWriter.tableBufferRoot(COLUMN_BUFFER_SIZE);
                    for (Object[] row : rows) {
                        table.addRow(row);
                    }
                    table.complete();
                    return currentWriter.writeNext().get();
                } catch (Exception failure) {
                    Throwable cause = unwrap(failure);
                    if (cause instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }

                    // Any write failure makes the current stream unsuitable for replay. Rebuild it even when the
                    // failure is not retryable so close() cannot accidentally reuse a failed stream.
                    closeCurrentWriter(failure);

                    if (!isRetryable(cause) || retry >= MAX_RETRIES) {
                        throw failure;
                    }

                    long delayMillis = retryDelayMillis(retry);
                    LOG.warn(
                            "Transient bulk write failure. Rebuilding the writer and retrying ({}/{}) in {} ms",
                            retry + 1,
                            MAX_RETRIES,
                            delayMillis,
                            cause);
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        interrupted.addSuppressed(failure);
                        throw interrupted;
                    }
                }
            }
        }

        private BulkStreamWriter getOrCreateWriter() {
            if (this.writer == null) {
                this.writer = this.greptimeDB.bulkStreamWriter(this.schema, this.config, this.context);
            }
            return this.writer;
        }

        private void completed() throws Exception {
            if (this.completed) {
                return;
            }

            BulkStreamWriter currentWriter = getOrCreateWriter();
            try {
                currentWriter.completed();
                this.writer = null;
                this.completed = true;
            } catch (Exception failure) {
                closeCurrentWriter(failure);
                throw failure;
            }
        }

        @Override
        public void close() throws Exception {
            if (this.writer != null) {
                BulkStreamWriter currentWriter = this.writer;
                this.writer = null;
                currentWriter.close();
            }
        }

        private void closeCurrentWriter(Throwable failure) {
            if (this.writer == null) {
                return;
            }

            BulkStreamWriter currentWriter = this.writer;
            this.writer = null;
            try {
                currentWriter.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static boolean isRetryable(Throwable failure) {
        // writeNext().get() has no caller-supplied wait timeout, so this is the configured per-message deadline.
        if (failure instanceof TimeoutException) {
            return true;
        }
        if (!(failure instanceof FlightRuntimeException)) {
            return false;
        }

        FlightStatusCode code = ((FlightRuntimeException) failure).status().code();
        return code == FlightStatusCode.UNAVAILABLE || code == FlightStatusCode.TIMED_OUT;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof ExecutionException || current instanceof CompletionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static long retryDelayMillis(int retry) {
        long delayMillis = INITIAL_RETRY_DELAY_MILLIS;
        for (int i = 0; i < retry; i++) {
            delayMillis = Math.min(delayMillis * 2, MAX_RETRY_DELAY_MILLIS);
        }

        long lowerBound = Math.max(1, delayMillis / 2);
        return ThreadLocalRandom.current().nextLong(lowerBound, delayMillis + 1);
    }
}
