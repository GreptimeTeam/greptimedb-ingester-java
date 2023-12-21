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

import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Table schema for write.
 * 
 * @author jiachun.fjc
 */
public class TableSchema {

    private static final Map<String, TableSchema> TABLE_SCHEMA_CACHE = new ConcurrentHashMap<>();

    private String tableName;
    private List<String> columnNames;
    private List<Common.SemanticType> semanticTypes;
    private List<Common.ColumnDataType> dataTypes;
    private List<Common.ColumnDataTypeExtension> dataTypeExtensions;

    private TableSchema() {}

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Common.SemanticType> getSemanticTypes() {
        return semanticTypes;
    }

    public List<Common.ColumnDataType> getDataTypes() {
        return dataTypes;
    }

    public List<Common.ColumnDataTypeExtension> getDataTypeExtensions() {
        return dataTypeExtensions;
    }

    public static TableSchema findSchema(String tableName) {
        return TABLE_SCHEMA_CACHE.get(tableName);
    }

    public static TableSchema removeSchema(String tableName) {
        return TABLE_SCHEMA_CACHE.remove(tableName);
    }

    public static void clearAllSchemas() {
        TABLE_SCHEMA_CACHE.clear();
    }

    public static Builder newBuilder(String tableName) {
        return new Builder(tableName);
    }

    public static class Builder {
        private final String tableName;
        private final List<String> columnNames = new ArrayList<>();
        private final List<Common.SemanticType> semanticTypes = new ArrayList<>();
        private final List<Common.ColumnDataType> dataTypes = new ArrayList<>();
        private final List<Common.ColumnDataTypeExtension> dataTypeExtensions = new ArrayList<>();

        public Builder(String tableName) {
            this.tableName = tableName;
        }

        public Builder addColumn(String name, SemanticType semanticType, DataType dataType) {
            return addColumn(name, semanticType, dataType, null);
        }

        public Builder addColumn(String name, SemanticType semanticType, DataType dataType,
                DataType.DecimalTypeExtension decimalTypeExtension) {
            Ensures.ensureNonNull(name, "Null column name");
            Ensures.ensureNonNull(semanticType, "Null semantic type");
            Ensures.ensureNonNull(dataType, "Null data type");

            this.columnNames.add(name);
            this.semanticTypes.add(semanticType.toProtoValue());
            this.dataTypes.add(dataType.toProtoValue());
            this.dataTypeExtensions.add(decimalTypeExtension == null ? Common.ColumnDataTypeExtension
                    .getDefaultInstance() : Common.ColumnDataTypeExtension.newBuilder()
                    .setDecimalType(decimalTypeExtension.into()).build());
            return this;
        }

        public TableSchema build() {
            Ensures.ensureNonNull(this.tableName, "Null table name");
            Ensures.ensureNonNull(this.columnNames, "Null column names");
            Ensures.ensureNonNull(this.semanticTypes, "Null semantic types");
            Ensures.ensureNonNull(this.dataTypes, "Null data types");

            int columnCount = this.columnNames.size();

            Ensures.ensure(columnCount > 0, "Empty column names");
            Ensures.ensure(columnCount == this.semanticTypes.size(),
                    "Column names size not equal to semantic types size");
            Ensures.ensure(columnCount == this.dataTypes.size(), "Column names size not equal to data types size");
            Ensures.ensure(columnCount == this.dataTypeExtensions.size(),
                    "Column names size not equal to data type extensions size");

            TableSchema tableSchema = new TableSchema();
            tableSchema.tableName = this.tableName;
            tableSchema.columnNames = this.columnNames;
            tableSchema.semanticTypes = this.semanticTypes;
            tableSchema.dataTypes = this.dataTypes;
            tableSchema.dataTypeExtensions = this.dataTypeExtensions;
            return tableSchema;
        }

        public TableSchema buildAndCache() {
            TableSchema tableSchema = build();
            TABLE_SCHEMA_CACHE.putIfAbsent(tableSchema.tableName, tableSchema);
            return tableSchema;
        }
    }
}
