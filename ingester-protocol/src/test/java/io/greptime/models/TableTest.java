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

import io.greptime.v1.RowData;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class TableTest {

    @Test
    public void testTableNonNull() {
        TableSchema schema = TableSchema.newBuilder("test_table") //
                .addColumn("col1", SemanticType.Tag, DataType.String) //
                .addColumn("col2", SemanticType.Tag, DataType.String) //
                .addColumn("col3", SemanticType.Field, DataType.Int32) //
                .build();

        Table.RowBasedTable table = (Table.RowBasedTable) Table.from(schema);
        table.addRow("1", "11", 111) //
                .addRow("2", "22", 222) //
                .addRow("3", "33", 333);

        Assert.assertEquals(3, table.rowCount());
        RowData.Rows rawRows = table.into();
        Assert.assertEquals(111, rawRows.getRows(0).getValues(2).getI32Value());
        Assert.assertEquals(222, rawRows.getRows(1).getValues(2).getI32Value());
        Assert.assertEquals(333, rawRows.getRows(2).getValues(2).getI32Value());
    }

    @Test
    public void testTableSomeNull() {
        TableSchema schema = TableSchema.newBuilder("test_table") //
                .addColumn("col1", SemanticType.Tag, DataType.String) //
                .addColumn("col2", SemanticType.Tag, DataType.String) //
                .addColumn("col3", SemanticType.Field, DataType.Int32) //
                .build();

        Table.RowBasedTable table = (Table.RowBasedTable) Table.from(schema);
        table.addRow("1", "11", 111) //
                .addRow("2", null, 222) //
                .addRow("3", "33", null);

        Assert.assertEquals(3, table.rowCount());
        RowData.Rows rawRows = table.into();
        Assert.assertEquals(111, rawRows.getRows(0).getValues(2).getI32Value());
        Assert.assertEquals(222, rawRows.getRows(1).getValues(2).getI32Value());
        Assert.assertFalse(rawRows.getRows(2).getValues(2).hasI32Value());
        Assert.assertFalse(rawRows.getRows(1).getValues(1).hasStringValue());
    }
}
