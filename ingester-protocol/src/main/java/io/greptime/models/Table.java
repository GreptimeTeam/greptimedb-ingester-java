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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

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
     * The bytes used by the table.
     */
    long bytesUsed();

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
     * Returns true if the table has been completed.
     */
    boolean isCompleted();

    /**
     * Convert to {@link Database.RowInsertRequest}.
     *
     * @return {@link Database.RowInsertRequest}
     */
    default Database.RowInsertRequest intoRowInsertRequest() {
        throw new UnsupportedOperationException("Not supported for this table type");
    }

    /**
     * Convert to {@link Database.RowDeleteRequest}.
     *
     * @return {@link Database.RowDeleteRequest}
     */
    default Database.RowDeleteRequest intoRowDeleteRequest() {
        throw new UnsupportedOperationException("Not supported for this table type");
    }

    default void checkNumValues(int len) {
        int columnCount = columnCount();
        Ensures.ensure(columnCount == len, "Expected values num: %d, actual: %d", columnCount, len);
    }

    /**
     * `TableBufferRoot` is an internal interface that represents a reference to table data stored in direct memory.
     * It provides access to the underlying memory allocation for efficient bulk data operations.
     *
     * <p>
     * `TableBufferRoot` is not thread-safe.
     * </p>
     */
    interface TableBufferRoot extends Table {}

    /**
     * Create a table from a table schema.
     *
     * @param tableSchema the table schema
     * @return a table
     */
    static Table from(TableSchema tableSchema) {
        return new Builder(tableSchema).build();
    }

    /**
     * Create a bulk table buffer root from a table schema and a vector schema root.
     *
     * @param tableSchema the table schema
     * @param root the vector schema root
     * @return a table buffer root
     */
    static TableBufferRoot tableBufferRoot(TableSchema tableSchema, VectorSchemaRoot root) {
        return new BulkTableBuilder(tableSchema, root).build();
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
            RowBasedTable table = new RowBasedTable(tableName, new ArrayList<>(columnCount));

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

        private final String tableName;
        private final List<RowData.ColumnSchema> columnSchemas;
        private final List<RowData.Row> rows;

        public RowBasedTable(String tableName, List<RowData.ColumnSchema> columnSchemas) {
            this(tableName, columnSchemas, new ArrayList<>());
        }

        private RowBasedTable(String tableName, List<RowData.ColumnSchema> columnSchemas, List<RowData.Row> rows) {
            this.tableName = tableName;
            this.columnSchemas = columnSchemas;
            this.rows = rows;
        }

        @Override
        public String tableName() {
            return this.tableName;
        }

        @Override
        public int rowCount() {
            return this.rows.size();
        }

        @Override
        public int columnCount() {
            return this.columnSchemas.size();
        }

        /**
         * <p>
         * This is an expensive operation, only used for testing
         * </p>
         */
        @Override
        public long bytesUsed() {
            return this.rows.stream().mapToLong(row -> row.getSerializedSize()).sum();
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

        @Override
        public boolean isCompleted() {
            return this.completed;
        }
    }

    class BulkTableBuilder {
        private final TableSchema tableSchema;
        private final VectorSchemaRoot root;

        public BulkTableBuilder(TableSchema tableSchema, VectorSchemaRoot root) {
            this.tableSchema = tableSchema;
            this.root = root;
        }

        public BulkTable build() {
            String tableName = this.tableSchema.getTableName();
            List<Common.ColumnDataType> dataTypes = this.tableSchema.getDataTypes();
            List<Common.ColumnDataTypeExtension> dataTypeExtensions = this.tableSchema.getDataTypeExtensions();

            Ensures.ensureNonNull(tableName, "Null table name");
            Ensures.ensureNonNull(dataTypes, "Null data types");

            int columnCount = dataTypes.size();

            Ensures.ensure(columnCount > 0, "Empty column data types");
            Ensures.ensure(
                    columnCount == dataTypeExtensions.size(),
                    "Column data types size not equal to data type extensions size");

            return new BulkTable(tableName, dataTypes, dataTypeExtensions, this.root);
        }
    }

    class BulkTable implements TableBufferRoot {

        private volatile boolean completed = false;

        private final String tableName;
        private final List<Common.ColumnDataType> dataTypes;
        private final List<Common.ColumnDataTypeExtension> dataTypeExtensions;
        private final VectorSchemaRoot root;

        public BulkTable(
                String tableName,
                List<Common.ColumnDataType> dataTypes,
                List<Common.ColumnDataTypeExtension> dataTypeExtensions,
                VectorSchemaRoot root) {
            this.tableName = tableName;
            this.dataTypes = dataTypes;
            this.dataTypeExtensions = dataTypeExtensions;
            this.root = root;
        }

        @Override
        public String tableName() {
            return this.tableName;
        }

        @Override
        public int rowCount() {
            return this.root.getRowCount();
        }

        @Override
        public int columnCount() {
            return this.root.getSchema().getFields().size();
        }

        @Override
        public long bytesUsed() {
            return this.root.getFieldVectors().stream()
                    .mapToLong(vector -> vector.getBufferSize())
                    .sum();
        }

        @Override
        public Table addRow(Object... values) {
            Ensures.ensure(
                    !this.completed,
                    "Table data construction has been completed. Cannot add more rows. Please create a new table instance.");

            checkNumValues(values.length);

            int rowCount = this.root.getRowCount();
            for (int i = 0; i < values.length; i++) {
                FieldVector vector = this.root.getVector(i);
                if (vector.getValueCapacity() < rowCount + 1) {
                    vector.reAlloc();
                }
                ArrowHelper.addValue(
                        vector, rowCount, this.dataTypes.get(i), this.dataTypeExtensions.get(i), values[i]);
            }
            this.root.setRowCount(rowCount + 1);
            return this;
        }

        @Override
        public Table subRange(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException("Unsupported method 'subRange' by BulkTable");
        }

        @Override
        public Table complete() {
            this.completed = true;
            return this;
        }

        @Override
        public boolean isCompleted() {
            return this.completed;
        }
    }
}
