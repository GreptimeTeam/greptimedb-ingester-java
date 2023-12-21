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
 * Data in row format, ready to be written to the DB.
 *
 * @author jiachun.fjc
 */
public interface TableRows {

    /**
     * The table name to write.
     */
    String tableName();

    /**
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
     * Insets one row.
     */
    TableRows insert(Object... values);

    TableRows subRange(int fromIndex, int toIndex);

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

    static TableRows from(TableSchema tableSchema) {
        return new Builder(tableSchema).build();
    }

    class Builder {
        private final TableSchema tableSchema;

        public Builder(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
        }

        public TableRows build() {
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
            Ensures.ensure(columnCount == dataTypeExtensions.size(), "Column names size not equal to data type extensions size");

            return buildRow(tableName, columnCount, columnNames, semanticTypes, dataTypes, dataTypeExtensions);
        }

        private static TableRows buildRow(String tableName, //
                                          int columnCount, //
                                          List<String> columnNames, //
                                          List<Common.SemanticType> semanticTypes, //
                                          List<Common.ColumnDataType> dataTypes, //
                                          List<Common.ColumnDataTypeExtension> dataTypeExtensions) {
            RowBasedTableRows rows = new RowBasedTableRows();
            rows.tableName = tableName;
            rows.columnSchemas = new ArrayList<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                RowData.ColumnSchema.Builder builder = RowData.ColumnSchema.newBuilder();
                builder.setColumnName(columnNames.get(i)) //
                        .setSemanticType(semanticTypes.get(i)) //
                        .setDatatype(dataTypes.get(i)) //
                        .setDatatypeExtension(dataTypeExtensions.get(i));
                rows.columnSchemas.add(builder.build());
            }
            return rows;
        }
    }

    class RowBasedTableRows implements TableRows, Into<RowData.Rows> {

        private String tableName;

        private List<RowData.ColumnSchema> columnSchemas;
        private final List<RowData.Row> rows;

        public RowBasedTableRows() {
            this.rows = new ArrayList<>();
        }

        private RowBasedTableRows(String tableName, List<RowData.ColumnSchema> columnSchemas, List<RowData.Row> rows) {
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
        public TableRows insert(Object... values) {
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
        public TableRows subRange(int fromIndex, int toIndex) {
            List<RowData.Row> rows = this.rows.subList(fromIndex, toIndex);
            return new RowBasedTableRows(this.tableName, this.columnSchemas, rows);
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            return Database.RowInsertRequest.newBuilder() //
                    .setTableName(this.tableName) //
                    .setRows(into()) //
                    .build();
        }

        @Override
        public Database.RowDeleteRequest intoRowDeleteRequest() {
            return Database.RowDeleteRequest.newBuilder() //
                    .setTableName(this.tableName) //
                    .setRows(into()) //
                    .build();
        }

        @Override
        public RowData.Rows into() {
            return RowData.Rows.newBuilder() //
                    .addAllSchema(this.columnSchemas) //
                    .addAllRows(this.rows) //
                    .build();
        }
    }
}
