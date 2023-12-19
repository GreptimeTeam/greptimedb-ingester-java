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

/**
 * `DataType` with extension info, now only used for `Decimal128`.
 *
 * @author jiachun.fjc
 */
public class DataTypeWithExtension {
    private final DataType columnDataType;
    private final DataType.DecimalTypeExtension decimalTypeExtension;

    public static DataTypeWithExtension of(DataType columnDataType) {
        if (columnDataType == DataType.Decimal128) {
            return new DataTypeWithExtension(columnDataType, DataType.DecimalTypeExtension.DEFAULT);
        }
        return new DataTypeWithExtension(columnDataType, null);
    }

    public static DataTypeWithExtension of(DataType columnDataType, DataType.DecimalTypeExtension decimalTypeExtension) {
        if (columnDataType == DataType.Decimal128) {
            return new DataTypeWithExtension(columnDataType,
                    decimalTypeExtension == null ? DataType.DecimalTypeExtension.DEFAULT : decimalTypeExtension);
        }
        return new DataTypeWithExtension(columnDataType, null);
    }

    DataTypeWithExtension(DataType columnDataType, DataType.DecimalTypeExtension decimalTypeExtension) {
        this.columnDataType = columnDataType;
        this.decimalTypeExtension = decimalTypeExtension;
    }

    public DataType getColumnDataType() {
        return columnDataType;
    }

    public DataType.DecimalTypeExtension getDecimalTypeExtension() {
        return decimalTypeExtension;
    }
}
