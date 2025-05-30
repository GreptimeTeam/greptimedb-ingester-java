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

import com.google.protobuf.UnsafeByteOperations;
import io.greptime.v1.Common;
import io.greptime.v1.RowData;
import java.util.concurrent.TimeUnit;

/**
 * A utility that handles some processing of row based data.
 */
public final class RowHelper {

    public static void addValue(
            RowData.Row.Builder builder,
            Common.ColumnDataType dataType,
            Common.ColumnDataTypeExtension dataTypeExtension,
            Object value) {
        RowData.Value.Builder valueBuilder = RowData.Value.newBuilder();
        if (value == null) {
            builder.addValues(valueBuilder.build());
            return;
        }

        switch (dataType) {
            case INT8:
                valueBuilder.setI8Value((int) value);
                break;
            case INT16:
                valueBuilder.setI16Value((int) value);
                break;
            case INT32:
                valueBuilder.setI32Value((int) value);
                break;
            case INT64:
                valueBuilder.setI64Value(ValueUtil.getLongValue(value));
                break;
            case UINT8:
                valueBuilder.setU8Value((int) value);
                break;
            case UINT16:
                valueBuilder.setU16Value((int) value);
                break;
            case UINT32:
                valueBuilder.setU32Value(ValueUtil.getIntValue(value));
                break;
            case UINT64:
                valueBuilder.setU64Value(ValueUtil.getLongValue(value));
                break;
            case FLOAT32:
                valueBuilder.setF32Value(((Number) value).floatValue());
                break;
            case FLOAT64:
                valueBuilder.setF64Value(((Number) value).doubleValue());
                break;
            case BOOLEAN:
                valueBuilder.setBoolValue((boolean) value);
                break;
            case BINARY:
                valueBuilder.setBinaryValue(UnsafeByteOperations.unsafeWrap((byte[]) value));
                break;
            case STRING:
                valueBuilder.setStringValue((String) value);
                break;
            case DATE:
                valueBuilder.setDateValue(ValueUtil.getDateValue(value));
                break;
            case TIMESTAMP_SECOND:
                valueBuilder.setTimestampSecondValue(ValueUtil.getTimestamp(value, TimeUnit.SECONDS));
                break;
            case TIMESTAMP_MILLISECOND:
                valueBuilder.setTimestampMillisecondValue(ValueUtil.getTimestamp(value, TimeUnit.MILLISECONDS));
                break;
            case TIMESTAMP_MICROSECOND:
                valueBuilder.setTimestampMicrosecondValue(ValueUtil.getTimestamp(value, TimeUnit.MICROSECONDS));
                break;
            case TIMESTAMP_NANOSECOND:
                valueBuilder.setTimestampNanosecondValue(ValueUtil.getTimestamp(value, TimeUnit.NANOSECONDS));
                break;
            case TIME_SECOND:
                valueBuilder.setTimeSecondValue(ValueUtil.getLongValue(value));
                break;
            case TIME_MILLISECOND:
                valueBuilder.setTimeMillisecondValue(ValueUtil.getLongValue(value));
                break;
            case TIME_MICROSECOND:
                valueBuilder.setTimeMicrosecondValue(ValueUtil.getLongValue(value));
                break;
            case TIME_NANOSECOND:
                valueBuilder.setTimeNanosecondValue(ValueUtil.getLongValue(value));
                break;
            case DECIMAL128:
                valueBuilder.setDecimal128Value(ValueUtil.getDecimal128Value(dataTypeExtension, value));
                break;
            case JSON:
                valueBuilder.setStringValue(ValueUtil.getJsonString(value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported `data_type`: %s", dataType));
        }

        builder.addValues(valueBuilder.build());
    }

    private RowHelper() {}
}
