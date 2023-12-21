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
public class TableRowsTest {

    @Test
    public void testWriteRowsNonNull() {
        TableSchema schema = TableSchema.newBuilder(TableName.with("", "test_table")) //
                .columnNames("col1", "col2", "col3") //
                .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
                .dataTypes(DataType.String, DataType.String, DataType.Int32) //
                .build();

        TableRows.RowBasedTableRows rows = (TableRows.RowBasedTableRows) TableRows.newBuilder(schema).build();
        rows.insert("1", "11", 111) //
                .insert("2", "22", 222) //
                .insert("3", "33", 333);

        Assert.assertEquals(3, rows.rowCount());
        RowData.Rows rawRows = rows.into();
        Assert.assertEquals(111, rawRows.getRows(0).getValues(2).getI32Value());
        Assert.assertEquals(222, rawRows.getRows(1).getValues(2).getI32Value());
        Assert.assertEquals(333, rawRows.getRows(2).getValues(2).getI32Value());
    }

    @Test
    public void testWriteRowsSomeNull() {
        TableSchema schema = TableSchema.newBuilder(TableName.with("", "test_table")) //
                .columnNames("col1", "col2", "col3") //
                .semanticTypes(SemanticType.Tag, SemanticType.Tag, SemanticType.Field) //
                .dataTypes(DataType.String, DataType.String, DataType.Int32) //
                .build();

        TableRows.RowBasedTableRows rows = (TableRows.RowBasedTableRows) TableRows.newBuilder(schema).build();
        rows.insert("1", "11", 111) //
                .insert("2", null, 222) //
                .insert("3", "33", null);

        Assert.assertEquals(3, rows.rowCount());
        RowData.Rows rawRows = rows.into();
        Assert.assertEquals(111, rawRows.getRows(0).getValues(2).getI32Value());
        Assert.assertEquals(222, rawRows.getRows(1).getValues(2).getI32Value());
        Assert.assertFalse(rawRows.getRows(2).getValues(2).hasI32Value());
        Assert.assertFalse(rawRows.getRows(1).getValues(1).hasStringValue());
    }
}
