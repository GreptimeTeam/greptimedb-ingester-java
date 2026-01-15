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

import static org.junit.Assert.assertEquals;
import io.greptime.models.DataType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import java.sql.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Bulk Write API.
 */
public class BulkWriteIT {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteIT.class);
    private static final int ROW_COUNT = 100;

    private static GreptimeDB client;
    private static Connection jdbcConn;

    private String tableName;
    private TableSchema schema;

    @BeforeClass
    public static void setupClass() throws Exception {
        client = ITHelper.createClient();
        jdbcConn = ITHelper.getJdbcConnection();
        LOG.info("Integration test client initialized");
    }

    @AfterClass
    public static void teardownClass() {
        if (client != null) {
            client.shutdownGracefully();
        }
        if (jdbcConn != null) {
            try {
                jdbcConn.close();
            } catch (Exception e) {
                LOG.warn("Failed to close JDBC connection", e);
            }
        }
    }

    @Before
    public void setup() throws Exception {
        tableName = ITHelper.uniqueTableName("test_bulk_write");
        schema = TableSchema.newBuilder(tableName)
                .addTag("host", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("cpu_user", DataType.Float64)
                .addField("cpu_sys", DataType.Float64)
                .build();

        // Bulk Write requires pre-existing table, create via DDL
        String createTableSql = String.format(
                "CREATE TABLE %s ("
                        + "host STRING,"
                        + "ts TIMESTAMP(3) TIME INDEX,"
                        + "cpu_user DOUBLE,"
                        + "cpu_sys DOUBLE,"
                        + "PRIMARY KEY (host)"
                        + ")",
                tableName);
        ITHelper.executeUpdate(jdbcConn, createTableSql);
        LOG.info("Created table: {}", tableName);
    }

    @After
    public void teardown() {
        ITHelper.dropTableIfExists(jdbcConn, tableName);
    }

    @Test
    public void testBulkWrite() throws Exception {
        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(64 * 1024 * 1024L)
                .timeoutMsPerMessage(30000)
                .maxRequestsInFlight(4)
                .build();

        try (BulkStreamWriter writer = client.bulkStreamWriter(schema, cfg)) {
            // Prepare test data with deterministic values
            Table.TableBufferRoot table = writer.tableBufferRoot(128);
            long baseTs = 1700000000000L;

            for (int i = 0; i < ROW_COUNT; i++) {
                String host = "host_" + i;
                long ts = baseTs + i * 1000;
                double cpuUser = i * 0.1;
                double cpuSys = i * 0.05;
                table.addRow(host, ts, cpuUser, cpuSys);
            }
            table.complete();

            // Write data
            Integer affectedRows = writer.writeNext().get();
            LOG.info("Bulk write affected rows: {}", affectedRows);

            // Verify exact row count, not just > 0
            assertEquals("Should write exact row count", ROW_COUNT, affectedRows.intValue());

            writer.completed();
        }

        // Verify row count via JDBC
        int count = ITHelper.queryCount(jdbcConn, tableName);
        assertEquals("Row count should match", ROW_COUNT, count);

        // Verify actual data content - spot check first, middle, and last rows
        ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0);
        ITHelper.verifyRow(jdbcConn, tableName, "host_50", 5.0, 2.5);
        ITHelper.verifyRow(jdbcConn, tableName, "host_99", 9.9, 4.95);

        LOG.info("Verified {} rows with correct data in table {}", count, tableName);
    }

    @Test
    public void testBulkWriteMultipleBatches() throws Exception {
        BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(64 * 1024 * 1024L)
                .timeoutMsPerMessage(30000)
                .maxRequestsInFlight(4)
                .build();

        int batchCount = 5;
        int rowsPerBatch = 20;
        int totalWritten = 0;

        try (BulkStreamWriter writer = client.bulkStreamWriter(schema, cfg)) {
            long baseTs = 1700000000000L;

            for (int batch = 0; batch < batchCount; batch++) {
                Table.TableBufferRoot table = writer.tableBufferRoot(64);

                for (int i = 0; i < rowsPerBatch; i++) {
                    int rowNum = batch * rowsPerBatch + i;
                    String host = "host_" + rowNum;
                    long ts = baseTs + rowNum * 1000;
                    double cpuUser = rowNum * 0.1;
                    double cpuSys = rowNum * 0.05;
                    table.addRow(host, ts, cpuUser, cpuSys);
                }
                table.complete();

                Integer affectedRows = writer.writeNext().get();
                // Verify each batch writes exact count
                assertEquals("Batch " + batch + " should write exact count", rowsPerBatch, affectedRows.intValue());
                totalWritten += affectedRows;
                LOG.info("Batch {} wrote {} rows", batch, affectedRows);
            }

            writer.completed();
        }

        // Verify total written count
        int expectedCount = batchCount * rowsPerBatch;
        assertEquals("Total written should match", expectedCount, totalWritten);

        // Verify row count via JDBC
        int count = ITHelper.queryCount(jdbcConn, tableName);
        assertEquals("Row count should match", expectedCount, count);

        // Verify actual data content across batches
        ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0); // first row of batch 0
        ITHelper.verifyRow(jdbcConn, tableName, "host_40", 4.0, 2.0); // first row of batch 2
        ITHelper.verifyRow(jdbcConn, tableName, "host_99", 9.9, 4.95); // last row

        LOG.info("Verified {} rows with correct data in table {}", count, tableName);
    }
}
