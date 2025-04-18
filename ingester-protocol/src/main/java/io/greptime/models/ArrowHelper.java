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

import io.greptime.ArrowCompressionType;
import io.greptime.common.util.Ensures;
import io.greptime.rpc.Context;
import io.greptime.v1.Common;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.TimeMicroHolder;
import org.apache.arrow.vector.holders.TimeMilliHolder;
import org.apache.arrow.vector.holders.TimeNanoHolder;
import org.apache.arrow.vector.holders.TimeSecHolder;
import org.apache.arrow.vector.holders.TimeStampMicroHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.holders.TimeStampNanoHolder;
import org.apache.arrow.vector.holders.TimeStampSecHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Helper class for Arrow schema creation.
 */
public class ArrowHelper {

    /**
     * Get the Arrow compression type from the context.
     *
     * @param ctx the context
     * @return the Arrow compression type
     */
    public static ArrowCompressionType getArrowCompressionType(Context ctx) {
        switch (ctx.getCompression()) {
            case Zstd:
                return ArrowCompressionType.Zstd;
            case Lz4:
                return ArrowCompressionType.Lz4;
            case None:
                return ArrowCompressionType.None;
            default:
                throw new IllegalArgumentException("Unsupported compression type: " + ctx.getCompression());
        }
    }

    /**
     * Create an Arrow schema from a table schema.
     *
     * @param tableSchema the table schema
     * @return the Arrow schema
     */
    public static Schema createSchema(TableSchema tableSchema) {
        Ensures.ensureNonNull(tableSchema, "tableSchema is null");

        int columnCount = tableSchema.getColumnNames().size();

        List<Field> fields = new ArrayList<>(columnCount);

        List<String> columnNames = tableSchema.getColumnNames();
        List<Common.ColumnDataType> dataTypes = tableSchema.getDataTypes();
        List<Common.ColumnDataTypeExtension> dataTypeExtensions = tableSchema.getDataTypeExtensions();

        for (int i = 0; i < columnCount; i++) {
            String name = columnNames.get(i);
            ArrowType type = convertToArrowType(dataTypes.get(i), dataTypeExtensions.get(i));

            Field field = Field.nullable(name, type);
            fields.add(field);
        }

        return new Schema(fields);
    }

    public static void addValue(
            FieldVector vector,
            int rowIndex,
            Common.ColumnDataType dataType,
            Common.ColumnDataTypeExtension dataTypeExtension,
            Object value) {
        if (value == null) {
            vector.setNull(rowIndex);
            return;
        }

        switch (dataType) {
            case INT8:
                ((TinyIntVector) vector).setSafe(rowIndex, (int) value);
                break;
            case INT16:
                ((SmallIntVector) vector).setSafe(rowIndex, (int) value);
                break;
            case INT32:
                ((IntVector) vector).setSafe(rowIndex, (int) value);
                break;
            case INT64:
                ((BigIntVector) vector).setSafe(rowIndex, (long) value);
                break;
            case UINT8:
                ((UInt1Vector) vector).setSafe(rowIndex, (int) value);
                break;
            case UINT16:
                ((UInt2Vector) vector).setSafe(rowIndex, (int) value);
                break;
            case UINT32:
                ((UInt4Vector) vector).setSafe(rowIndex, ((Long) value).intValue());
                break;
            case UINT64:
                ((UInt8Vector) vector).setSafe(rowIndex, (long) value);
                break;
            case FLOAT32:
                ((Float4Vector) vector).setSafe(rowIndex, (float) value);
                break;
            case FLOAT64:
                ((Float8Vector) vector).setSafe(rowIndex, (double) value);
                break;
            case BOOLEAN:
                ((BitVector) vector).setSafe(rowIndex, (boolean) value ? 1 : 0);
                break;
            case BINARY:
                ((VarBinaryVector) vector).setSafe(rowIndex, (byte[]) value);
                break;
            case STRING:
                ((VarCharVector) vector).setSafe(rowIndex, ((String) value).getBytes(StandardCharsets.UTF_8));
                break;
            case DATE:
                ((DateDayVector) vector).setSafe(rowIndex, ValueUtil.getDateValue(value));
                break;
            case TIMESTAMP_SECOND: {
                TimeStampSecHolder holder = new TimeStampSecHolder();
                holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.SECONDS);
                ((TimeStampSecVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIMESTAMP_MILLISECOND: {
                TimeStampMilliHolder holder = new TimeStampMilliHolder();
                holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.MILLISECONDS);
                ((TimeStampMilliVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIMESTAMP_MICROSECOND: {
                TimeStampMicroHolder holder = new TimeStampMicroHolder();
                holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.MICROSECONDS);
                ((TimeStampMicroVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIMESTAMP_NANOSECOND: {
                TimeStampNanoHolder holder = new TimeStampNanoHolder();
                holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.NANOSECONDS);
                ((TimeStampNanoVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIME_SECOND: {
                TimeSecHolder holder = new TimeSecHolder();
                holder.value = (int) ValueUtil.getLongValue(value);
                ((TimeSecVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIME_MILLISECOND: {
                TimeMilliHolder holder = new TimeMilliHolder();
                holder.value = (int) ValueUtil.getLongValue(value);
                ((TimeMilliVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIME_MICROSECOND: {
                TimeMicroHolder holder = new TimeMicroHolder();
                holder.value = ValueUtil.getLongValue(value);
                ((TimeMicroVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case TIME_NANOSECOND: {
                TimeNanoHolder holder = new TimeNanoHolder();
                holder.value = ValueUtil.getLongValue(value);
                ((TimeNanoVector) vector).setSafe(rowIndex, holder);
                break;
            }
            case DECIMAL128:
                byte[] bytes = ValueUtil.getDecimal128BigEndianBytes(dataTypeExtension, value);
                ((DecimalVector) vector).setBigEndianSafe(rowIndex, bytes);
                break;
            case JSON:
                byte[] jsonBytes = ValueUtil.getJsonString(value).getBytes(StandardCharsets.UTF_8);
                ((VarCharVector) vector).setSafe(rowIndex, jsonBytes);
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    static ArrowType convertToArrowType(
            Common.ColumnDataType dataType, Common.ColumnDataTypeExtension dataTypeExtension) {
        switch (dataType) {
            case INT8:
                return new ArrowType.Int(8, true);
            case INT16:
                return new ArrowType.Int(16, true);
            case INT32:
                return new ArrowType.Int(32, true);
            case INT64:
                return new ArrowType.Int(64, true);
            case UINT8:
                return new ArrowType.Int(8, false);
            case UINT16:
                return new ArrowType.Int(16, false);
            case UINT32:
                return new ArrowType.Int(32, false);
            case UINT64:
                return new ArrowType.Int(64, false);
            case FLOAT32:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case FLOAT64:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BOOLEAN:
                return new ArrowType.Bool();
            case BINARY:
                return new ArrowType.Binary();
            case STRING:
                return new ArrowType.Utf8();
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIMESTAMP_SECOND:
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            case TIMESTAMP_MILLISECOND:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case DATETIME:
                // DateTime is an alias of TIMESTAMP_MICROSECOND
                // https://github.com/GreptimeTeam/greptimedb/issues/5489
            case TIMESTAMP_MICROSECOND:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            case TIMESTAMP_NANOSECOND:
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            case TIME_SECOND:
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            case TIME_MILLISECOND:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIME_MICROSECOND:
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            case TIME_NANOSECOND:
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            case DECIMAL128:
                Ensures.ensureNonNull(dataTypeExtension, "dataTypeExtension is null");
                Common.DecimalTypeExtension decimalTypeExtension = dataTypeExtension.getDecimalType();
                Ensures.ensureNonNull(decimalTypeExtension, "decimalTypeExtension is null");
                return new ArrowType.Decimal(decimalTypeExtension.getPrecision(), decimalTypeExtension.getScale(), 128);
            case JSON:
                return new ArrowType.Utf8();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private ArrowHelper() {}
}
