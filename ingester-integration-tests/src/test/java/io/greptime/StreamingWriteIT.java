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
import io.greptime.models.WriteOk;
import java.sql.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Streaming Write API.
 */
public class StreamingWriteIT {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingWriteIT.class);
    private static final int ROW_COUNT = 10;

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
    public void setup() {
        tableName = ITHelper.uniqueTableName("test_streaming_write");
        schema = TableSchema.newBuilder(tableName)
                .addTag("host", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("cpu_user", DataType.Float64)
                .addField("cpu_sys", DataType.Float64)
                .build();
    }

    @After
    public void teardown() {
        ITHelper.dropTableIfExists(jdbcConn, tableName);
    }

    @Test
    public void testStreamingWrite() throws Exception {
        // Create stream writer
        StreamWriter<Table, WriteOk> writer = client.streamWriter();

        // Prepare and write test data with deterministic values
        long baseTs = 1700000000000L;

        for (int i = 0; i < ROW_COUNT; i++) {
            Table table = Table.from(schema);
            String host = "host_" + i;
            long ts = baseTs + i * 1000;
            double cpuUser = i * 0.1;
            double cpuSys = i * 0.05;
            table.addRow(host, ts, cpuUser, cpuSys);
            table.complete();

            writer.write(table);
        }

        // Complete the stream
        WriteOk result = writer.completed().get();

        // Verify write result - check exact count, not just > 0
        assertEquals("Should write exact row count", ROW_COUNT, result.getSuccess());
        LOG.info("Stream write result: {}", result);

        // Verify row count via JDBC
        int count = ITHelper.queryCount(jdbcConn, tableName);
        assertEquals("Row count should match", ROW_COUNT, count);

        // Verify actual data content
        ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0);
        ITHelper.verifyRow(jdbcConn, tableName, "host_5", 0.5, 0.25);
        ITHelper.verifyRow(jdbcConn, tableName, "host_9", 0.9, 0.45);

        LOG.info("Verified {} rows with correct data in table {}", count, tableName);
    }

    @Test
    public void testStreamingWriteMultipleTables() throws Exception {
        String tableName2 = ITHelper.uniqueTableName("test_streaming_write_2");
        TableSchema schema2 = TableSchema.newBuilder(tableName2)
                .addTag("region", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("mem_usage", DataType.Float64)
                .build();

        try {
            // Create stream writer
            StreamWriter<Table, WriteOk> writer = client.streamWriter();

            // Write data to multiple tables with deterministic values
            long baseTs = 1700000000000L;

            for (int i = 0; i < ROW_COUNT; i++) {
                Table table1 = Table.from(schema);
                table1.addRow("host_" + i, baseTs + i * 1000, i * 0.1, i * 0.05);
                table1.complete();
                writer.write(table1);

                Table table2 = Table.from(schema2);
                table2.addRow("region_" + i, baseTs + i * 1000, i * 0.2);
                table2.complete();
                writer.write(table2);
            }

            // Complete the stream
            WriteOk result = writer.completed().get();

            // Verify write result - check exact count
            assertEquals("Should write exact row count", ROW_COUNT * 2, result.getSuccess());
            LOG.info("Stream write result: {}", result);

            // Verify row counts via JDBC
            assertEquals("Table 1 row count", ROW_COUNT, ITHelper.queryCount(jdbcConn, tableName));
            assertEquals("Table 2 row count", ROW_COUNT, ITHelper.queryCount(jdbcConn, tableName2));

            // Verify actual data content
            ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0);
            ITHelper.verifyRow(jdbcConn, tableName, "host_9", 0.9, 0.45);

            LOG.info("Verified {} rows with correct data in each table", ROW_COUNT);
        } finally {
            ITHelper.dropTableIfExists(jdbcConn, tableName2);
        }
    }
}
