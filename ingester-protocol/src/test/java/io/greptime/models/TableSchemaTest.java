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

import io.greptime.TestUtil;
import io.greptime.v1.RowData;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;

/**
 * @author jiachun.fjc
 */
public class TableSchemaTest {

    @Test
    public void testNonNull() {
        TableSchema schema = TableSchema.newBuilder("test_table") //
                .addTag("col1", DataType.String) //
                .addTag("col2", DataType.String) //
                .addField("col3", DataType.Int32) //
                .addColumn("col4", SemanticType.Field, DataType.Decimal128, new DataType.DecimalTypeExtension(39, 9)) //
                .build();

        Table.RowBasedTable table = (Table.RowBasedTable) Table.from(schema);
        table.addRow("1", "11", 111, new BigDecimal("0.1")) //
                .addRow("2", "22", 222, new BigDecimal("0.2")) //
                .addRow("3", "33", 333, new BigDecimal("0.3"));

        Assert.assertEquals(3, table.rowCount());
        RowData.Rows rawRows = table.into();
        Assert.assertEquals(111, rawRows.getRows(0).getValues(2).getI32Value());
        Assert.assertEquals(222, rawRows.getRows(1).getValues(2).getI32Value());
        Assert.assertEquals(333, rawRows.getRows(2).getValues(2).getI32Value());
        Assert.assertEquals(new BigDecimal("0.100000000"),
                TestUtil.getDecimal(rawRows.getRows(0).getValues(3).getDecimal128Value(), 9));
        Assert.assertEquals(new BigDecimal("0.200000000"),
                TestUtil.getDecimal(rawRows.getRows(1).getValues(3).getDecimal128Value(), 9));
        Assert.assertEquals(new BigDecimal("0.300000000"),
                TestUtil.getDecimal(rawRows.getRows(2).getValues(3).getDecimal128Value(), 9));
    }

    @Test
    public void testSomeNull() {
        TableSchema schema = TableSchema.newBuilder("test_table") //
                .addTag("col1", DataType.String) //
                .addTag("col2", DataType.String) //
                .addField("col3", DataType.Int32) //
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


    @Test
    public void testNotSupportTimestamp() {
        TableSchema.Builder builder = TableSchema.newBuilder("test_table") //
                .addTag("col1", DataType.String) //
                .addTag("col2", DataType.String) //
                .addField("col3", DataType.Int32);

        try {
            builder.addTimestamp("col4", DataType.Int32);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Invalid timestamp data type"));
        }
    }

    @Test
    public void testNotSupportDecimalExtension() {
        TableSchema.Builder builder = TableSchema.newBuilder("test_table") //
                .addTag("col1", DataType.String) //
                .addTag("col2", DataType.String) //
                .addField("col3", DataType.Int32);

        try {
            builder.addColumn("col4", SemanticType.Field, DataType.Float64, new DataType.DecimalTypeExtension(39, 9));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Only decimal type can have decimal type extension"));
        }
    }
}
