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

package io.greptime.models;

import org.junit.Assert;
import org.junit.Test;

public class TableTest {

    private Table newTestTable() {
        TableSchema schema = TableSchema.newBuilder("my_table")
                .addTag("tag1", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("field1", DataType.Float64)
                .build();

        return Table.from(schema);
    }

    @Test
    public void testTablePointCount() {
        Table table = newTestTable();
        table.addRow("tag1", System.currentTimeMillis(), 1.0);
        table.complete();

        Assert.assertEquals(3, table.pointCount());
    }

    @Test
    public void testSubRange() {
        Table table = newTestTable();
        table.addRow("tag1", System.currentTimeMillis(), 1.0);
        table.addRow("tag1", System.currentTimeMillis(), 2.0);
        table.complete();

        Table subRange = table.subRange(0, 1);
        Assert.assertEquals(1, subRange.rowCount());
        Assert.assertEquals(3, subRange.columnCount());
    }

    @Test
    public void testAddRowAfterComplete() {
        Table table = newTestTable();
        table.addRow("tag1", System.currentTimeMillis(), 1.0);
        table.complete();

        Assert.assertTrue(table.isCompleted());

        try {
            table.addRow("tag1", System.currentTimeMillis(), 2.0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Table data construction has been completed"));
        }
    }
}
