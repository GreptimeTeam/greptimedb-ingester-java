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
import static org.junit.Assert.assertTrue;
import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
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
 * Integration tests for Regular Write API.
 */
public class RegularWriteIT {

    private static final Logger LOG = LoggerFactory.getLogger(RegularWriteIT.class);
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
        tableName = ITHelper.uniqueTableName("test_regular_write");
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
    public void testRegularWrite() throws Exception {
        Table table = Table.from(schema);
        long baseTs = 1700000000000L;

        for (int i = 0; i < ROW_COUNT; i++) {
            table.addRow("host_" + i, baseTs + i * 1000, i * 0.1, i * 0.05);
        }
        table.complete();

        Result<WriteOk, Err> result = client.write(table).get();

        assertTrue("Write should succeed", result.isOk());
        assertEquals(ROW_COUNT, result.getOk().getSuccess());

        assertEquals(ROW_COUNT, ITHelper.queryCount(jdbcConn, tableName));
        ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0);
        ITHelper.verifyRow(jdbcConn, tableName, "host_5", 0.5, 0.25);
        ITHelper.verifyRow(jdbcConn, tableName, "host_9", 0.9, 0.45);
    }

    @Test
    public void testRegularWriteMultipleTables() throws Exception {
        String tableName2 = ITHelper.uniqueTableName("test_regular_write_2");
        TableSchema schema2 = TableSchema.newBuilder(tableName2)
                .addTag("region", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("mem_usage", DataType.Float64)
                .build();

        try {
            Table table1 = Table.from(schema);
            Table table2 = Table.from(schema2);
            long baseTs = 1700000000000L;

            for (int i = 0; i < ROW_COUNT; i++) {
                table1.addRow("host_" + i, baseTs + i * 1000, i * 0.1, i * 0.05);
                table2.addRow("region_" + i, baseTs + i * 1000, i * 0.2);
            }
            table1.complete();
            table2.complete();

            Result<WriteOk, Err> result = client.write(table1, table2).get();

            assertTrue("Write should succeed", result.isOk());
            assertEquals(ROW_COUNT * 2, result.getOk().getSuccess());
            assertEquals(ROW_COUNT, ITHelper.queryCount(jdbcConn, tableName));
            assertEquals(ROW_COUNT, ITHelper.queryCount(jdbcConn, tableName2));
            ITHelper.verifyRow(jdbcConn, tableName, "host_0", 0.0, 0.0);
            ITHelper.verifyRow(jdbcConn, tableName, "host_9", 0.9, 0.45);
        } finally {
            ITHelper.dropTableIfExists(jdbcConn, tableName2);
        }
    }

    @Test
    public void testRegularWriteWithNullValues() throws Exception {
        Table table = Table.from(schema);
        long baseTs = 1700000000000L;

        table.addRow("host_0", baseTs, 1.0, 0.5);
        table.addRow("host_1", baseTs + 1000, null, 0.5); // null cpu_user
        table.addRow("host_2", baseTs + 2000, 1.0, null); // null cpu_sys
        table.addRow("host_3", baseTs + 3000, null, null); // both null
        table.complete();

        Result<WriteOk, Err> result = client.write(table).get();

        assertTrue("Write should succeed", result.isOk());
        assertEquals(4, result.getOk().getSuccess());
        assertEquals(4, ITHelper.queryCount(jdbcConn, tableName));
        ITHelper.verifyRow(jdbcConn, tableName, "host_0", 1.0, 0.5);
    }
}
