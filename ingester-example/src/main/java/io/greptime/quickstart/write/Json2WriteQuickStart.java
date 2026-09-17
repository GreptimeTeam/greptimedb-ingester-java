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

package io.greptime.quickstart.write;

import io.greptime.GreptimeDB;
import io.greptime.WriteOp;
import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.quickstart.TestConnector;
import io.greptime.rpc.Context;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes JSON2 values with the regular write API. Requires GreptimeDB 1.2.1 or later.
 * Run this main method with the connection settings in db-connection.properties.
 * The first insert automatically creates json2_logs with append_mode=true.
 * JSON2 is also supported by streaming row writes, but not Bulk/Arrow writes.
 */
public class Json2WriteQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(Json2WriteQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();
        try {
            TableSchema schema = TableSchema.newBuilder("json2_logs")
                    .addTimestamp("ts", DataType.TimestampMillisecond)
                    .addField("payload", DataType.Json2)
                    .build();

            Table table = Table.from(schema);
            long ts = System.currentTimeMillis();
            // JSON strings are parsed into native Protobuf values before sending.
            // The top-level value must be an object or null; nested arrays and scalars are supported.
            table.addRow(ts, "{\"message\":\"hello\",\"nested\":{\"items\":[1,\"two\",null]},\"ok\":true}");
            table.addRow(ts + 1, "{}");
            // Java null and the JSON string "null" both represent SQL NULL.
            table.addRow(ts + 2, null);
            table.addRow(ts + 3, "null");
            // Java objects can also be serialized by the client's existing Gson encoder.
            table.addRow(ts + 4, Collections.singletonMap("message", "from Java"));

            // JSON2 requires append_mode=true when the table is created.
            Context ctx = Context.newDefault().withHint("append_mode", "true");
            Result<WriteOk, Err> result = greptimeDB
                    .write(Collections.singletonList(table.complete()), WriteOp.Insert, ctx)
                    .get();
            if (!result.isOk()) {
                throw new IllegalStateException(
                        "JSON2 write failed", result.getErr().getError());
            }
            if (result.getOk().getSuccess() != 5) {
                throw new IllegalStateException(
                        "Expected 5 inserted rows, got " + result.getOk().getSuccess());
            }
            LOG.info("Inserted {} JSON2 rows", result.getOk().getSuccess());
        } finally {
            greptimeDB.shutdownGracefully();
        }
    }
}
