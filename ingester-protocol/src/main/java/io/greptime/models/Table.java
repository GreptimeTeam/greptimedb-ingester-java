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

import io.greptime.common.Into;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import io.greptime.v1.RowData;
import java.util.ArrayList;
import java.util.List;

/**
 * A table represents data in row format that can be written to the database.
 *
 * <p>
 * The table is mutable and allows adding rows incrementally. However, the data cannot be written to
 * the database until the table is marked as complete by calling {@link #complete()}. This design enables
 * batch processing of rows before writing.
 * </p>
 *
 * <p>
 * Important notes:
 * </p>
 * <ul>
 *   <li>Tables are not thread-safe</li>
 *   <li>Tables cannot be reused - create a new instance for each write operation</li>
 *   <li>The associated {@link TableSchema} is immutable and can be reused</li>
 * </ul>
 *
 * <p>
 * Example usage:
 * </p>
 * <pre>{@code
 * TableSchema schema = TableSchema.newBuilder("my_table")
 *     .addTag("tag1", DataType.String)
 *     .addTimestamp("ts", DataType.TimestampMillisecond)
 *     .addField("field1", DataType.Float64)
 *     .build();
 *
 * Table table = Table.from(schema);
 * // The order of the values must match the schema definition.
 * table.addRow(1, "2023-01-01 00:00:00", "value1");
 * table.addRow(2, "2023-01-01 00:00:01", "value2");
 * table.complete();
 * }</pre>
 */
public interface Table {

    /**
     * The table name to write.
     */
    String tableName();

    /**··
     * The rows count to write.
     */
    int rowCount();

    /**
     * The columns count to write.
     */
    int columnCount();

    /**
     * The points count to write.
     */
    default int pointCount() {
        return rowCount() * columnCount();
    }

    /**
     * Insets one row with all columns.
     *
     * The order of the values must be the same as the order of the schema.
     */
    Table addRow(Object... values);

    Table subRange(int fromIndex, int toIndex);

    /**
     * Completes the table data construction and prevents further row additions.
     * After calling this method, the table will be immutable and ready to be written
     * to the database.
     */
    Table complete();

    /**
     * Convert to {@link Database.RowInsertRequest}.
     *
     * @return {@link Database.RowInsertRequest}
     */
    Database.RowInsertRequest intoRowInsertRequest();

    /**
     * Convert to {@link Database.RowDeleteRequest}.
     *
     * @return {@link Database.RowDeleteRequest}
     */
    Database.RowDeleteRequest intoRowDeleteRequest();

    default void checkNumValues(int len) {
        int columnCount = columnCount();
        Ensures.ensure(columnCount == len, "Expected values num: %d, actual: %d", columnCount, len);
    }

    static Table from(TableSchema tableSchema) {
        return new Builder(tableSchema).build();
    }

    class Builder {
        private final TableSchema tableSchema;

        public Builder(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
        }

        public Table build() {
            String tableName = this.tableSchema.getTableName();
            List<String> columnNames = this.tableSchema.getColumnNames();
            List<Common.SemanticType> semanticTypes = this.tableSchema.getSemanticTypes();
            List<Common.ColumnDataType> dataTypes = this.tableSchema.getDataTypes();
            List<Common.ColumnDataTypeExtension> dataTypeExtensions = this.tableSchema.getDataTypeExtensions();

            Ensures.ensureNonNull(tableName, "Null table name");
            Ensures.ensureNonNull(columnNames, "Null column names");
            Ensures.ensureNonNull(semanticTypes, "Null semantic types");
            Ensures.ensureNonNull(dataTypes, "Null data types");

            int columnCount = columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == semanticTypes.size(), "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == dataTypes.size(), "Column names size not equal to data types size");
            Ensures.ensure(
                    columnCount == dataTypeExtensions.size(),
                    "Column names size not equal to data type extensions size");

            return buildTable(tableName, columnCount, columnNames, semanticTypes, dataTypes, dataTypeExtensions);
        }

        private static Table buildTable(
                String tableName,
                int columnCount,
                List<String> columnNames,
                List<Common.SemanticType> semanticTypes,
                List<Common.ColumnDataType> dataTypes,
                List<Common.ColumnDataTypeExtension> dataTypeExtensions) {
            RowBasedTable table = new RowBasedTable();
            table.tableName = tableName;
            table.columnSchemas = new ArrayList<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                RowData.ColumnSchema.Builder builder = RowData.ColumnSchema.newBuilder();
                builder.setColumnName(columnNames.get(i))
                        .setSemanticType(semanticTypes.get(i))
                        .setDatatype(dataTypes.get(i))
                        .setDatatypeExtension(dataTypeExtensions.get(i));
                table.columnSchemas.add(builder.build());
            }
            return table;
        }
    }

    class RowBasedTable implements Table, Into<RowData.Rows> {

        private volatile boolean completed = false;

        private String tableName;

        private List<RowData.ColumnSchema> columnSchemas;
        private final List<RowData.Row> rows;

        public RowBasedTable() {
            this.rows = new ArrayList<>();
        }

        private RowBasedTable(String tableName, List<RowData.ColumnSchema> columnSchemas, List<RowData.Row> rows) {
            this.tableName = tableName;
            this.columnSchemas = columnSchemas;
            this.rows = rows;
        }

        @Override
        public String tableName() {
            return tableName;
        }

        @Override
        public int rowCount() {
            return rows.size();
        }

        @Override
        public int columnCount() {
            return columnSchemas.size();
        }

        @Override
        public Table addRow(Object... values) {
            Ensures.ensure(
                    !this.completed,
                    "Table data construction has been completed. Cannot add more rows. Please create a new table instance.");

            checkNumValues(values.length);

            RowData.Row.Builder rowBuilder = RowData.Row.newBuilder();
            for (int i = 0; i < values.length; i++) {
                RowData.ColumnSchema columnSchema = this.columnSchemas.get(i);
                Object value = values[i];
                RowHelper.addValue(rowBuilder, columnSchema.getDatatype(), columnSchema.getDatatypeExtension(), value);
            }
            this.rows.add(rowBuilder.build());

            return this;
        }

        @Override
        public Table subRange(int fromIndex, int toIndex) {
            List<RowData.Row> rows = this.rows.subList(fromIndex, toIndex);
            return new RowBasedTable(this.tableName, this.columnSchemas, rows);
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            return Database.RowInsertRequest.newBuilder()
                    .setTableName(this.tableName)
                    .setRows(into())
                    .build();
        }

        @Override
        public Database.RowDeleteRequest intoRowDeleteRequest() {
            return Database.RowDeleteRequest.newBuilder()
                    .setTableName(this.tableName)
                    .setRows(into())
                    .build();
        }

        @Override
        public RowData.Rows into() {
            return RowData.Rows.newBuilder()
                    .addAllSchema(this.columnSchemas)
                    .addAllRows(this.rows)
                    .build();
        }

        @Override
        public Table complete() {
            this.completed = true;
            return this;
        }
    }
}
