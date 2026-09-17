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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import com.google.gson.JsonParser;
import io.greptime.CachedPojoObjectMapper;
import io.greptime.v1.Common;
import io.greptime.v1.RowData;
import java.util.Collections;
import org.junit.Test;

public class Json2Test {
    private TableSchema schema() {
        return TableSchema.newBuilder("json2_logs")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("payload", DataType.Json2)
                .build();
    }

    @Test
    public void rowEncoding() throws Exception {
        Table table = Table.from(schema());
        table.addRow(
                0L,
                "{\"items\":[true,-9223372036854775808,18446744073709551615,1.5,\"你好\",null,{},[],{\"ok\":false}]}");
        table.addRow(1L, "{}");
        table.addRow(2L, "null");
        table.addRow(3L, null);
        table.addRow(4L, Collections.singletonMap("n", 42));
        RowData.Rows rows = table.complete().intoRowInsertRequest().getRows();
        assertEquals(rows, RowData.Rows.parseFrom(rows.toByteArray()));

        RowData.ColumnSchema column = rows.getSchema(1);
        assertEquals(Common.ColumnDataType.JSON, column.getDatatype());
        assertEquals(Common.SemanticType.FIELD, column.getSemanticType());
        assertTrue(column.getDatatypeExtension().hasJsonNativeType());
        assertEquals(
                Common.ColumnDataType.JSON,
                column.getDatatypeExtension().getJsonNativeType().getDatatype());
        assertFalse(column.getDatatypeExtension().getJsonNativeType().hasDatatypeExtension());
        assertEquals("greptime.json2", column.getOptions().getOptionsOrThrow("ARROW:extension:name"));
        assertEquals(
                JsonParser.parseString(
                        "{\"json_settings\":{\"type_hints\":[],\"max_auto_expanded_paths\":100},\"layout_version\":2}"),
                JsonParser.parseString(column.getOptions().getOptionsOrThrow("ARROW:extension:metadata")));

        RowData.JsonObject.Entry entry =
                rows.getRows(0).getValues(1).getJsonValue().getObject().getEntries(0);
        assertEquals("items", entry.getKey());
        RowData.JsonList items = entry.getValue().getArray();
        assertEquals(9, items.getItemsCount());
        assertEquals(RowData.JsonValue.newBuilder().setBoolean(true).build(), items.getItems(0));
        assertEquals(RowData.JsonValue.newBuilder().setInt(Long.MIN_VALUE).build(), items.getItems(1));
        assertEquals(RowData.JsonValue.newBuilder().setUint(-1L).build(), items.getItems(2));
        assertEquals(RowData.JsonValue.newBuilder().setFloat(1.5).build(), items.getItems(3));
        assertEquals(RowData.JsonValue.newBuilder().setStr("你好").build(), items.getItems(4));
        assertEquals(RowData.JsonValue.getDefaultInstance(), items.getItems(5));
        assertEquals(
                RowData.JsonValue.newBuilder()
                        .setObject(RowData.JsonObject.getDefaultInstance())
                        .build(),
                items.getItems(6));
        assertEquals(
                RowData.JsonValue.newBuilder()
                        .setArray(RowData.JsonList.getDefaultInstance())
                        .build(),
                items.getItems(7));
        assertEquals("ok", items.getItems(8).getObject().getEntries(0).getKey());
        assertEquals(
                RowData.JsonValue.newBuilder().setBoolean(false).build(),
                items.getItems(8).getObject().getEntries(0).getValue());
        assertEquals(items.getItems(6), rows.getRows(1).getValues(1).getJsonValue());
        assertEquals(RowData.Value.getDefaultInstance(), rows.getRows(2).getValues(1));
        assertEquals(RowData.Value.getDefaultInstance(), rows.getRows(3).getValues(1));
        assertEquals(
                RowData.JsonValue.newBuilder().setUint(42).build(),
                rows.getRows(4)
                        .getValues(1)
                        .getJsonValue()
                        .getObject()
                        .getEntries(0)
                        .getValue());
    }

    @Test
    public void rejectsInvalidInputWithoutAddingRows() {
        Table table = Table.from(schema());
        for (String invalid : new String[] {
            "",
            " ",
            "{",
            "{} trailing",
            "{} {}",
            "null true",
            "[]",
            "1",
            "true",
            "\"text\"",
            "{\"n\":1e400}",
            "{a:1}",
            "{'a':1}",
            "{\"a\":NaN}",
            "{\"a\":01}",
            "{\"a\":/*comment*/1}"
        }) {
            assertThrows(invalid, IllegalArgumentException.class, () -> table.addRow(0L, invalid));
            assertEquals(0, table.rowCount());
        }
        assertThrows(IllegalArgumentException.class, () -> TableSchema.newBuilder("t")
                .addTag("j", DataType.Json2));
        assertThrows(IllegalArgumentException.class, () -> ArrowHelper.createSchema(schema()));
    }

    @Test
    public void pojoAndLegacyJson() {
        Table table = new CachedPojoObjectMapper().mapToTable(Collections.singletonList(new Json2Log()));
        RowData.Rows pojoRows = table.intoRowInsertRequest().getRows();
        int payloadIndex = pojoRows.getSchema(0).getColumnName().equals("payload") ? 0 : 1;
        assertTrue(pojoRows.getSchema(payloadIndex).getDatatypeExtension().hasJsonNativeType());
        assertTrue(pojoRows.getRows(0).getValues(payloadIndex).hasJsonValue());

        Table legacy = Table.from(
                TableSchema.newBuilder("legacy").addField("j", DataType.Json).build());
        legacy.addRow("{\"n\":1}");
        RowData.Rows rows = legacy.intoRowInsertRequest().getRows();
        assertEquals(
                Common.JsonTypeExtension.JSON_BINARY,
                rows.getSchema(0).getDatatypeExtension().getJsonType());
        assertFalse(rows.getSchema(0).hasOptions());
        assertEquals("{\"n\":1}", rows.getRows(0).getValues(0).getStringValue());
    }

    @Metric(name = "json2_logs")
    public static class Json2Log {
        @Column(name = "ts", dataType = DataType.TimestampMillisecond, timestamp = true)
        long ts = 0L;

        @Column(name = "payload", dataType = DataType.Json2)
        String payload = "{}";
    }
}
