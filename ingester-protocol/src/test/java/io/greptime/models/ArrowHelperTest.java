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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class ArrowHelperTest {

    @Test
    public void testCreateSchema() {
        TableSchema tableSchema = TableSchema.newBuilder("my_table")
                .addTag("tag1", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("field1", DataType.Float64)
                .addField("field2", DataType.Int8)
                .addField("field3", DataType.Int16)
                .addField("field4", DataType.Int32)
                .addField("field5", DataType.Int64)
                .addField("field6", DataType.Float32)
                .addField("field7", DataType.Float64)
                .addField("field8", DataType.Bool)
                .addField("field9", DataType.Binary)
                .addField("field10", DataType.String)
                .addField("field11", DataType.Date)
                .addField("field12", DataType.TimestampSecond)
                .addField("field13", DataType.TimestampMillisecond)
                .addField("field14", DataType.TimestampMicrosecond)
                .addField("field15", DataType.TimestampNanosecond)
                .addField("field16", DataType.TimeSecond)
                .addField("field17", DataType.TimeMilliSecond)
                .addField("field18", DataType.TimeMicroSecond)
                .addField("field19", DataType.TimeNanoSecond)
                .addField("field20", DataType.Decimal128)
                .addField("field21", DataType.Json)
                .build();

        Schema schema = ArrowHelper.createSchema(tableSchema);
        Assert.assertEquals(23, schema.getFields().size());
        Assert.assertEquals("tag1", schema.getFields().get(0).getName());
        Assert.assertEquals("ts", schema.getFields().get(1).getName());
        Assert.assertEquals("field1", schema.getFields().get(2).getName());
        Assert.assertEquals("field2", schema.getFields().get(3).getName());
        Assert.assertEquals("field3", schema.getFields().get(4).getName());
        Assert.assertEquals("field4", schema.getFields().get(5).getName());
        Assert.assertEquals("field5", schema.getFields().get(6).getName());
        Assert.assertEquals("field6", schema.getFields().get(7).getName());
        Assert.assertEquals("field7", schema.getFields().get(8).getName());
        Assert.assertEquals("field8", schema.getFields().get(9).getName());
        Assert.assertEquals("field9", schema.getFields().get(10).getName());
        Assert.assertEquals("field10", schema.getFields().get(11).getName());
        Assert.assertEquals("field11", schema.getFields().get(12).getName());
        Assert.assertEquals("field12", schema.getFields().get(13).getName());
        Assert.assertEquals("field13", schema.getFields().get(14).getName());
        Assert.assertEquals("field14", schema.getFields().get(15).getName());
        Assert.assertEquals("field15", schema.getFields().get(16).getName());
        Assert.assertEquals("field16", schema.getFields().get(17).getName());
        Assert.assertEquals("field17", schema.getFields().get(18).getName());
        Assert.assertEquals("field18", schema.getFields().get(19).getName());
        Assert.assertEquals("field19", schema.getFields().get(20).getName());
        Assert.assertEquals("field20", schema.getFields().get(21).getName());
        Assert.assertEquals("field21", schema.getFields().get(22).getName());

        Assert.assertEquals(
                new ArrowType.Utf8().getTypeID(),
                schema.getFields().get(0).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null).getTypeID(),
                schema.getFields().get(1).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE).getTypeID(),
                schema.getFields().get(2).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Int(8, true).getTypeID(),
                schema.getFields().get(3).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Int(16, true).getTypeID(),
                schema.getFields().get(4).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Int(32, true).getTypeID(),
                schema.getFields().get(5).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Int(64, true).getTypeID(),
                schema.getFields().get(6).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE).getTypeID(),
                schema.getFields().get(7).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE).getTypeID(),
                schema.getFields().get(8).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Bool().getTypeID(),
                schema.getFields().get(9).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Binary().getTypeID(),
                schema.getFields().get(10).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Utf8().getTypeID(),
                schema.getFields().get(11).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Date(DateUnit.DAY).getTypeID(),
                schema.getFields().get(12).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Timestamp(TimeUnit.SECOND, null).getTypeID(),
                schema.getFields().get(13).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Timestamp(TimeUnit.MILLISECOND, null).getTypeID(),
                schema.getFields().get(14).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null).getTypeID(),
                schema.getFields().get(15).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Timestamp(TimeUnit.NANOSECOND, null).getTypeID(),
                schema.getFields().get(16).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Time(TimeUnit.SECOND, 32).getTypeID(),
                schema.getFields().get(17).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Time(TimeUnit.MILLISECOND, 32).getTypeID(),
                schema.getFields().get(18).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Time(TimeUnit.MICROSECOND, 64).getTypeID(),
                schema.getFields().get(19).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Time(TimeUnit.NANOSECOND, 64).getTypeID(),
                schema.getFields().get(20).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Decimal(38, 18, 128).getTypeID(),
                schema.getFields().get(21).getType().getTypeID());
        Assert.assertEquals(
                new ArrowType.Binary().getTypeID(),
                schema.getFields().get(22).getType().getTypeID());
    }
}
