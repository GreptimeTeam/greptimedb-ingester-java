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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.greptime.common.util.Ensures;
import io.greptime.v1.Common;
import io.greptime.v1.RowData;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ValueUtil {

    static int ONE_DAY_IN_SECONDS = 86400;

    static int getIntValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        }

        if (value instanceof Long) {
            return ((Long) value).intValue();
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        // Not null
        throw new IllegalArgumentException("Cannot convert value of type " + value.getClass() + " to int");
    }

    static long getLongValue(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        }

        if (value instanceof Long) {
            return (long) value;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        // Not null
        throw new IllegalArgumentException("Cannot convert value of type " + value.getClass() + " to long");
    }

    static int getDateValue(Object value) {
        if (value instanceof Instant) {
            long epochDay = ((Instant) value).getEpochSecond() / ONE_DAY_IN_SECONDS;
            return (int) epochDay;
        }

        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            long epochDay = instant.getEpochSecond() / ONE_DAY_IN_SECONDS;
            return (int) epochDay;
        }

        if (value instanceof LocalDate) {
            return (int) ((LocalDate) value).toEpochDay();
        }

        return (int) getLongValue(value);
    }

    static long getTimestamp(Object value, TimeUnit timeUnit) {
        if (value instanceof Instant) {
            return getTimestampFromInstant((Instant) value, timeUnit);
        }

        if (value instanceof Date) {
            Instant instant = ((Date) value).toInstant();
            return getTimestampFromInstant(instant, timeUnit);
        }

        return getLongValue(value);
    }

    static long getTimestampFromInstant(Instant value, TimeUnit timeUnit) {
        Ensures.ensureNonNull(value, "Instant value cannot be null");
        Ensures.ensureNonNull(timeUnit, "TimeUnit cannot be null");
        switch (timeUnit) {
            case SECONDS:
                return value.getEpochSecond();
            case MILLISECONDS:
                return value.toEpochMilli();
            case MICROSECONDS:
                return instantToMicros(value);
            case NANOSECONDS:
                return instantToNanos(value);
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    static long instantToMicros(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1000_000);
            long adjustment = nanos / 1000 - 1000_000;
            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1000_000);
            return Math.addExact(micros, nanos / 1000);
        }
    }

    static long instantToNanos(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();
        if (seconds < 0 && nanos > 0) {
            long nanosValue = Math.multiplyExact(seconds + 1, 1_000_000_000);
            long adjustment = nanos - 1_000_000_000;
            return Math.addExact(nanosValue, adjustment);
        } else {
            long nanosValue = Math.multiplyExact(seconds, 1_000_000_000);
            return Math.addExact(nanosValue, nanos);
        }
    }

    static Common.Decimal128 getDecimal128Value(Common.ColumnDataTypeExtension dataTypeExtension, Object value) {
        Ensures.ensure(value instanceof BigDecimal, "Expected type: `BigDecimal`, actual: %s", value.getClass());
        Ensures.ensureNonNull(dataTypeExtension, "Null `dataTypeExtension`");
        Common.DecimalTypeExtension decimalTypeExtension = dataTypeExtension.hasDecimalType()
                ? dataTypeExtension.getDecimalType()
                : DataType.DecimalTypeExtension.DEFAULT.into();
        BigDecimal decimal = (BigDecimal) value;
        BigDecimal converted = decimal.setScale(decimalTypeExtension.getScale(), RoundingMode.HALF_UP);

        BigInteger unscaledValue = converted.unscaledValue();
        long high64Bits = unscaledValue.shiftRight(64).longValue();
        long low64Bits = unscaledValue.longValue();

        return Common.Decimal128.newBuilder().setHi(high64Bits).setLo(low64Bits).build();
    }

    static byte[] getDecimal128BigEndianBytes(Common.ColumnDataTypeExtension dataTypeExtension, Object value) {
        Ensures.ensure(value instanceof BigDecimal, "Expected type: `BigDecimal`, actual: %s", value.getClass());
        Ensures.ensureNonNull(dataTypeExtension, "Null `dataTypeExtension`");
        Common.DecimalTypeExtension decimalTypeExtension = dataTypeExtension.hasDecimalType()
                ? dataTypeExtension.getDecimalType()
                : DataType.DecimalTypeExtension.DEFAULT.into();
        BigDecimal decimal = (BigDecimal) value;
        BigDecimal converted = decimal.setScale(decimalTypeExtension.getScale(), RoundingMode.HALF_UP);

        BigInteger unscaledValue = converted.unscaledValue();
        long high64Bits = unscaledValue.shiftRight(64).longValue();
        long low64Bits = unscaledValue.longValue();

        // Convert to big endian bytes
        byte[] bytes = new byte[16];
        bytes[0] = (byte) (high64Bits >> 56);
        bytes[1] = (byte) (high64Bits >> 48);
        bytes[2] = (byte) (high64Bits >> 40);
        bytes[3] = (byte) (high64Bits >> 32);
        bytes[4] = (byte) (high64Bits >> 24);
        bytes[5] = (byte) (high64Bits >> 16);
        bytes[6] = (byte) (high64Bits >> 8);
        bytes[7] = (byte) high64Bits;
        bytes[8] = (byte) (low64Bits >> 56);
        bytes[9] = (byte) (low64Bits >> 48);
        bytes[10] = (byte) (low64Bits >> 40);
        bytes[11] = (byte) (low64Bits >> 32);
        bytes[12] = (byte) (low64Bits >> 24);
        bytes[13] = (byte) (low64Bits >> 16);
        bytes[14] = (byte) (low64Bits >> 8);
        bytes[15] = (byte) low64Bits;

        return bytes;
    }

    // Gson's instances are Thread-safe we can reuse them freely across multiple threads.
    private static final Gson GSON = new Gson();

    // Read through the adapter: Gson.fromJson/JsonParser enable lenient parsing in Gson 2.9.
    static RowData.Value getJson2Value(Object value) {
        try (JsonReader reader = new JsonReader(new StringReader(getJsonString(value)))) {
            reader.setLenient(false);
            JsonElement json = GSON.getAdapter(JsonElement.class).read(reader);
            if (reader.peek() != JsonToken.END_DOCUMENT || (!json.isJsonObject() && !json.isJsonNull())) {
                throw new IllegalArgumentException("Invalid JSON2 value: expected a JSON object or null");
            }
            return json.isJsonNull()
                    ? RowData.Value.getDefaultInstance()
                    : RowData.Value.newBuilder().setJsonValue(encodeJson2(json)).build();
        } catch (IOException | NumberFormatException e) {
            throw new IllegalArgumentException("Invalid JSON2 value", e);
        }
    }

    private static RowData.JsonValue encodeJson2(JsonElement json) {
        RowData.JsonValue.Builder builder = RowData.JsonValue.newBuilder();
        if (json.isJsonObject()) {
            RowData.JsonObject.Builder object = RowData.JsonObject.newBuilder();
            for (Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
                object.addEntries(RowData.JsonObject.Entry.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(encodeJson2(entry.getValue())));
            }
            builder.setObject(object);
        } else if (json.isJsonArray()) {
            RowData.JsonList.Builder array = RowData.JsonList.newBuilder();
            for (JsonElement item : json.getAsJsonArray()) {
                array.addItems(encodeJson2(item));
            }
            builder.setArray(array);
        } else if (!json.isJsonNull()) {
            JsonPrimitive primitive = json.getAsJsonPrimitive();
            if (primitive.isBoolean()) {
                builder.setBoolean(primitive.getAsBoolean());
            } else if (primitive.isString()) {
                builder.setStr(primitive.getAsString());
            } else {
                String number = primitive.getAsString();
                if (number.indexOf('.') < 0 && number.indexOf('e') < 0 && number.indexOf('E') < 0) {
                    BigInteger integer = new BigInteger(number);
                    if (integer.signum() >= 0 && integer.bitLength() <= 64) {
                        return builder.setUint(integer.longValue()).build();
                    }
                    if (integer.signum() < 0 && integer.bitLength() <= 63) {
                        return builder.setInt(integer.longValue()).build();
                    }
                }
                double numberValue = primitive.getAsDouble();
                Ensures.ensure(Double.isFinite(numberValue), "Invalid JSON2 value: number is out of range");
                builder.setFloat(numberValue);
            }
        }
        return builder.build();
    }

    static String getJsonString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        return GSON.toJson(value);
    }
}
