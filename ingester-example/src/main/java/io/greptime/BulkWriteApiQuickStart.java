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

package io.greptime;

import io.greptime.BulkWrite.Config;
import io.greptime.common.util.StringBuilderHelper;
import io.greptime.models.DataType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.rpc.Compression;
import io.greptime.rpc.Context;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates how to use the bulk write API to write data to the database.
 */
public class BulkWriteApiQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriteApiQuickStart.class);

    public static void main(String[] args) throws Exception {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        TableSchema schema = TableSchema.newBuilder("my_table_with_greptime_all_data_types")
                .addTag("tag_string_1", DataType.String)
                .addTag("tag_string_2", DataType.String)
                .addTag("tag_string_3", DataType.String)
                .addTag("tag_string_4", DataType.String)
                .addTag("tag_string_5", DataType.String)
                .addTag("tag_string_6", DataType.String)
                .addTag("tag_string_7", DataType.String)
                .addTag("tag_string_8", DataType.String)
                .addTag("tag_string_9", DataType.String)
                .addTag("tag_string_10", DataType.String)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addField("field_int8", DataType.Int8)
                .addField("field_int16", DataType.Int16)
                .addField("field_int32", DataType.Int32)
                .addField("field_int64", DataType.Int64)
                .addField("field_uint8", DataType.UInt8)
                .addField("field_uint16", DataType.UInt16)
                .addField("field_uint32", DataType.UInt32)
                .addField("field_uint64", DataType.UInt64)
                .addField("field_float32", DataType.Float32)
                .addField("field_float64", DataType.Float64)
                .addField("field_boolean", DataType.Bool)
                .addField("field_binary", DataType.Binary)
                .addField("field_string", DataType.String)
                .addField("field_date", DataType.Date)
                .addField("field_timestamp_second", DataType.TimestampSecond)
                .addField("field_timestamp_millisecond", DataType.TimestampMillisecond)
                .addField("field_timestamp_microsecond", DataType.TimestampMicrosecond)
                .addField("field_timestamp_nanosecond", DataType.TimestampNanosecond)
                .addField("field_time_second", DataType.TimeSecond)
                .addField("field_time_millisecond", DataType.TimeMilliSecond)
                .addField("field_time_microsecond", DataType.TimeMicroSecond)
                .addField("field_time_nanosecond", DataType.TimeNanoSecond)
                .addField("field_decimal128", DataType.Decimal128)
                .addField("field_json", DataType.Json)
                .build();

        Config cfg = Config.newBuilder()
                .allocatorInitReservation(0)
                .allocatorMaxAllocation(1024 * 1024 * 1024L)
                .timeoutMsPerMessage(30000)
                .maxRequestsInFlight(8)
                .build();
        Context ctx = Context.newDefault().withCompression(Compression.None);

        // Bulk write api cannot auto create table
        Table toCreate = Table.from(schema);
        toCreate.addRow(generateOneRow(100000));
        toCreate.complete();
        greptimeDB.write(toCreate).get();

        try (BulkStreamWriter bulkStreamWriter = greptimeDB.bulkStreamWriter(schema, cfg, ctx)) {

            // Write 100 times, each time write 10000 rows
            for (int i = 0; i < 100; i++) {
                long start = System.currentTimeMillis();
                Table.TableBufferRoot table = bulkStreamWriter.tableBufferRoot(1024);
                for (int j = 0; j < 10000; j++) {
                    // with 100000 cardinality
                    Object[] row = generateOneRow(100000);
                    table.addRow(row);
                }
                // Complete the table; adding rows is no longer permitted.
                table.complete();

                LOG.info("Prepare data, time cost: {}ms", System.currentTimeMillis() - start);

                start = System.currentTimeMillis();
                CompletableFuture<Integer> future = bulkStreamWriter.writeNext();
                Integer result = future.get();
                LOG.info("Wrote rows: {}, time cost: {}ms", result, System.currentTimeMillis() - start);
            }

            bulkStreamWriter.completed();
        }
    }

    private static Object[] generateOneRow(int cardinality) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int tagCardinality = Math.max(1, cardinality / 10);
        byte[] binaryValue = new byte[random.nextInt(100)];
        random.nextBytes(binaryValue);

        // Generate a string between 1k and 2k characters
        StringBuilder stringBuilder = StringBuilderHelper.get();
        int stringLength = random.nextInt(1000, 2001); // Between 1000 and 2000 characters
        for (int i = 0; i < stringLength; i++) {
            stringBuilder.append((char) (random.nextInt(26) + 'a')); // Random lowercase letters
        }
        String stringValue =
                "Hello GreptimeDB, I am a random string with 1K ~ 2K characters: " + stringBuilder.toString();

        Instant timestampValue = java.time.LocalDateTime.now().toInstant(java.time.ZoneOffset.UTC);

        Map<String, String> jsonValue = new HashMap<>();
        jsonValue.put("key1", "value1");
        jsonValue.put("key2", "value2");
        jsonValue.put("key3", "value3");

        return new Object[] {
            String.format("tag_string_1_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_2_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_3_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_4_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_5_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_6_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_7_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_8_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_9_%d", random.nextInt(tagCardinality)),
            String.format("tag_string_10_%d", random.nextInt(tagCardinality)),
            System.currentTimeMillis(), // ts
            random.nextInt(127), // field_int8
            random.nextInt(32767), // field_int16
            null, // field_int32
            random.nextLong(), // field_int64
            random.nextInt(255), // field_uint8
            random.nextInt(65535), // field_uint16
            random.nextLong(4294967295L), // field_uint32
            random.nextLong(), // field_uint64
            random.nextFloat(), // field_float32
            random.nextDouble(), // field_float64
            random.nextBoolean(), // field_boolean
            binaryValue, // field_binary
            stringValue, // field_string with 1k-2k characters
            java.time.LocalDate.now(), // field_date
            timestampValue, // field_timestamp_second
            timestampValue, // field_timestamp_millisecond
            timestampValue, // field_timestamp_microsecond
            timestampValue, // field_timestamp_nanosecond
            1, // field_time_second
            2, // field_time_millisecond
            3, // field_time_microsecond
            4, // field_time_nanosecond
            BigDecimal.valueOf(random.nextDouble()), // field_decimal128
            jsonValue, // field_json
        };
    }
}
