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
import java.util.Iterator;
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

    public static void addValues(
            FieldVector vector,
            int startRowIndex,
            Common.ColumnDataType dataType,
            Common.ColumnDataTypeExtension dataTypeExtension,
            Iterator<Object> values) {
        switch (dataType) {
            case INT8:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((TinyIntVector) vector).setSafe(startRowIndex++, (int) value);
                    }
                }
                break;
            case INT16:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((SmallIntVector) vector).setSafe(startRowIndex++, (int) value);
                    }
                }
                break;
            case INT32:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((IntVector) vector).setSafe(startRowIndex++, (int) value);
                    }
                }
                break;
            case INT64:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((BigIntVector) vector).setSafe(startRowIndex++, (long) value);
                    }
                }
                break;
            case UINT8:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((UInt1Vector) vector).setSafe(startRowIndex++, (int) value);
                    }
                }
                break;
            case UINT16:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((UInt2Vector) vector).setSafe(startRowIndex++, (int) value);
                    }
                }
                break;
            case UINT32:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((UInt4Vector) vector).setSafe(startRowIndex++, ValueUtil.getIntValue(value));
                    }
                }
                break;
            case UINT64:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((UInt8Vector) vector).setSafe(startRowIndex++, (long) value);
                    }
                }
                break;
            case FLOAT32:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((Float4Vector) vector).setSafe(startRowIndex++, (float) value);
                    }
                }
                break;
            case FLOAT64:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((Float8Vector) vector).setSafe(startRowIndex++, (double) value);
                    }
                }
                break;
            case BOOLEAN:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((BitVector) vector).setSafe(startRowIndex++, (boolean) value ? 1 : 0);
                    }
                }
                break;
            case BINARY:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((VarBinaryVector) vector).setSafe(startRowIndex++, (byte[]) value);
                    }
                }
                break;
            case STRING:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((VarCharVector) vector)
                                .setSafe(startRowIndex++, ((String) value).getBytes(StandardCharsets.UTF_8));
                    }
                }
                break;
            case DATE:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        ((DateDayVector) vector).setSafe(startRowIndex++, ValueUtil.getDateValue(value));
                    }
                }
                break;
            case TIMESTAMP_SECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeStampSecHolder holder = new TimeStampSecHolder();
                        holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.SECONDS);
                        ((TimeStampSecVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIMESTAMP_MILLISECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeStampMilliHolder holder = new TimeStampMilliHolder();
                        holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.MILLISECONDS);
                        ((TimeStampMilliVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIMESTAMP_MICROSECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeStampMicroHolder holder = new TimeStampMicroHolder();
                        holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.MICROSECONDS);
                        ((TimeStampMicroVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIMESTAMP_NANOSECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeStampNanoHolder holder = new TimeStampNanoHolder();
                        holder.value = ValueUtil.getTimestamp(value, java.util.concurrent.TimeUnit.NANOSECONDS);
                        ((TimeStampNanoVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIME_SECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeSecHolder holder = new TimeSecHolder();
                        holder.value = (int) ValueUtil.getLongValue(value);
                        ((TimeSecVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIME_MILLISECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeMilliHolder holder = new TimeMilliHolder();
                        holder.value = (int) ValueUtil.getLongValue(value);
                        ((TimeMilliVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIME_MICROSECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeMicroHolder holder = new TimeMicroHolder();
                        holder.value = ValueUtil.getLongValue(value);
                        ((TimeMicroVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case TIME_NANOSECOND: {
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        TimeNanoHolder holder = new TimeNanoHolder();
                        holder.value = ValueUtil.getLongValue(value);
                        ((TimeNanoVector) vector).setSafe(startRowIndex++, holder);
                    }
                }
                break;
            }
            case DECIMAL128:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        byte[] bytes = ValueUtil.getDecimal128BigEndianBytes(dataTypeExtension, value);
                        ((DecimalVector) vector).setBigEndianSafe(startRowIndex++, bytes);
                    }
                }
                break;
            case JSON:
                while (values.hasNext()) {
                    Object value = values.next();
                    if (value == null) {
                        vector.setNull(startRowIndex++);
                    } else {
                        byte[] jsonBytes = ValueUtil.getJsonString(value).getBytes(StandardCharsets.UTF_8);
                        ((VarBinaryVector) vector).setSafe(startRowIndex++, jsonBytes);
                    }
                }
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
                return new ArrowType.Binary();
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    private ArrowHelper() {}
}
