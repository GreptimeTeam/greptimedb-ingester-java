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

import io.greptime.TestUtil;
import io.greptime.v1.Common;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ValueUtilTest {

    @Test
    public void testGetLongValue() {
        Assert.assertEquals(1L, ValueUtil.getLongValue(1));
        Assert.assertEquals(1L, ValueUtil.getLongValue(1L));
        Assert.assertEquals(1L, ValueUtil.getLongValue(1.0));
        Assert.assertEquals(1L, ValueUtil.getLongValue(1.0f));
        Assert.assertEquals(1L, ValueUtil.getLongValue(BigInteger.valueOf(1)));
        Assert.assertEquals(1L, ValueUtil.getLongValue(BigDecimal.valueOf(1)));
    }

    @Test
    public void testGetDateValue() {
        Calendar cal = Calendar.getInstance();
        TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");
        cal.setTimeZone(gmtTimeZone);
        cal.set(1970, Calendar.JANUARY, 2);
        Assert.assertEquals(1, ValueUtil.getDateValue(cal.getTime()));
        Assert.assertEquals(1, ValueUtil.getDateValue(Instant.ofEpochSecond(86400)));
        Assert.assertEquals(1, ValueUtil.getDateValue(LocalDate.ofEpochDay(1)));
        Assert.assertEquals(1, ValueUtil.getDateValue(1));
    }

    @Test
    public void testGetTimestampValue() {
        Calendar cal = Calendar.getInstance();
        TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");
        cal.setTimeZone(gmtTimeZone);
        cal.set(1970, Calendar.JANUARY, 2, 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 111);
        Assert.assertEquals(86400111, ValueUtil.getTimestamp(cal.getTime(), TimeUnit.MILLISECONDS));
        Assert.assertEquals(86400111, ValueUtil.getTimestamp(cal.getTime().toInstant(), TimeUnit.MILLISECONDS));
        Assert.assertEquals(86400000, ValueUtil.getTimestamp(Instant.ofEpochSecond(86400), TimeUnit.MILLISECONDS));
        Assert.assertEquals(86400, ValueUtil.getTimestamp(86400, TimeUnit.SECONDS));
    }

    @Test
    public void testInstantToMicros() {
        // Test positive instant
        Instant instant = Instant.parse("2024-03-20T10:15:30.123456789Z");
        long expectedMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        Assert.assertEquals(expectedMicros, ValueUtil.instantToMicros(instant));

        // Test negative instant
        Instant negativeInstant = Instant.parse("1969-12-31T23:59:59.999999999Z");
        Assert.assertEquals(-1, ValueUtil.instantToMicros(negativeInstant));
    }

    @Test
    public void testInstantToNanos() {
        // Test positive instant
        Instant instant = Instant.parse("2024-03-20T10:15:30.123456789Z");
        long expectedNanos = instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
        Assert.assertEquals(expectedNanos, ValueUtil.instantToNanos(instant));

        // Test negative instant
        Instant negativeInstant = Instant.parse("1969-12-31T23:59:59.999999999Z");
        Assert.assertEquals(-1, ValueUtil.instantToNanos(negativeInstant));
    }

    @Test
    public void testGetTimestampFromInstant() {
        // Create a specific timestamp for testing
        Instant instant = Instant.parse("2024-03-20T10:15:30.123456789Z");
        // Calculate expected values from the instant
        long expectedSeconds = instant.getEpochSecond();
        long expectedMillis = instant.toEpochMilli();
        long expectedMicros = instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        long expectedNanos = instant.getEpochSecond() * 1_000_000_000 + instant.getNano();
        // Test seconds
        Assert.assertEquals(expectedSeconds, ValueUtil.getTimestampFromInstant(instant, TimeUnit.SECONDS));
        // Test milliseconds
        Assert.assertEquals(expectedMillis, ValueUtil.getTimestampFromInstant(instant, TimeUnit.MILLISECONDS));
        // Test microseconds
        Assert.assertEquals(expectedMicros, ValueUtil.getTimestampFromInstant(instant, TimeUnit.MICROSECONDS));
        // Test nanoseconds
        Assert.assertEquals(expectedNanos, ValueUtil.getTimestampFromInstant(instant, TimeUnit.NANOSECONDS));
    }

    @Test
    public void testGetDecimal128Value() {
        final int precision = 38;
        final int scale = 9;

        Common.DecimalTypeExtension decimalTypeExtension = Common.DecimalTypeExtension.newBuilder()
                .setPrecision(precision)
                .setScale(scale)
                .build();
        Common.ColumnDataTypeExtension dataTypeExtension = Common.ColumnDataTypeExtension.newBuilder()
                .setDecimalType(decimalTypeExtension)
                .build();

        for (int i = 0; i < 1000; i++) {
            BigInteger bigInt = BigInteger.valueOf(new Random().nextLong()).shiftLeft(64);
            bigInt = bigInt.add(BigInteger.valueOf(new Random().nextLong()));
            BigDecimal value = new BigDecimal(bigInt, scale);
            Common.Decimal128 result = ValueUtil.getDecimal128Value(dataTypeExtension, value);

            BigDecimal value2 = TestUtil.getDecimal(result, scale);

            Assert.assertEquals(value, value2);
        }
    }

    @Test
    public void testGetJsonStringShouldReturnJsonStringForObject() {
        String jsonString = ValueUtil.getJsonString(new TestObject("test", 123));
        Assert.assertEquals("{\"name\":\"test\",\"value\":123}", jsonString);

        Map<String, Object> map = new HashMap<>();
        map.put("name", "test");
        map.put("value", 123);
        String jsonString2 = ValueUtil.getJsonString(map);
        Assert.assertEquals("{\"name\":\"test\",\"value\":123}", jsonString2);
    }

    @Test
    public void testGetJsonStringShouldReturnStringForString() {
        String jsonString = ValueUtil.getJsonString("test");
        Assert.assertEquals("test", jsonString);

        String jsonString2 = ValueUtil.getJsonString("{\"name\":\"test\",\"value\":123}");
        Assert.assertEquals("{\"name\":\"test\",\"value\":123}", jsonString2);
    }

    private static class TestObject {
        String name;
        int value;

        public TestObject(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }
}
