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

import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.SemanticType;
import io.greptime.models.TableRows;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.options.GreptimeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author jiachun.fjc
 */
public class WriteTableRowsQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(WriteTableRowsQuickStart.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // GreptimeDB has a default database named "public", we can use it as the test database
        String database = "public";
        // By default, GreptimeDB listens on port 4001 using the gRPC protocol.
        // We can provide multiple endpoints that point to the same GreptimeDB cluster.
        // The client will make calls to these endpoints based on a load balancing strategy.
        String[] endpoints = {"127.0.0.1:4001"};
        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database) //
                .build();

        GreptimeDB greptimeDB = new GreptimeDB();

        if (!greptimeDB.init(opts)) {
            throw new RuntimeException("Failed to start GreptimeDB client");
        }

        TableSchema myMetric3Schema = TableSchema.newBuilder("my_metric3") //
                .addColumn("tag1", SemanticType.Tag, DataType.String) //
                .addColumn("tag2", SemanticType.Tag, DataType.String) //
                .addColumn("tag3", SemanticType.Tag, DataType.String) //
                .addColumn("ts", SemanticType.Timestamp, DataType.TimestampMillisecond) //
                .addColumn("field1", SemanticType.Field, DataType.String) //
                .addColumn("field2", SemanticType.Field, DataType.Float64) //
                .addColumn("field3", SemanticType.Field, DataType.Decimal128) //
                .addColumn("field4", SemanticType.Field, DataType.Int32) //
                .build();

        TableSchema myMetric4Schema = TableSchema.newBuilder("my_metric4") //
                .addColumn("tag1", SemanticType.Tag, DataType.String) //
                .addColumn("tag2", SemanticType.Tag, DataType.String) //
                .addColumn("ts", SemanticType.Timestamp, DataType.TimestampSecond) //
                .addColumn("field1", SemanticType.Field, DataType.Date) //
                .addColumn("field2", SemanticType.Field, DataType.Float64) //
                .build();

        TableRows myMetric3Rows = TableRows.from(myMetric3Schema);
        TableRows myMetric4Rows = TableRows.from(myMetric4Schema);

        for (int i = 0; i < 10; i++) {
            String tag1v = "tag_value_1_" + i;
            String tag2v = "tag_value_2_" + i;
            String tag3v = "tag_value_3_" + i;
            long ts = System.currentTimeMillis();
            String field1 = "field_value_1" + i;
            double field2 = i + 0.1;
            BigDecimal field3 = new BigDecimal(i);
            int field4 = i + 1;

            myMetric3Rows.insert(tag1v, tag2v, tag3v, ts, field1, field2, field3, field4);
        }

        for (int i = 0; i < 10; i++) {
            String tag1v = "tag_value_1_" + i;
            String tag2v = "tag_value_2_" + i;
            long ts = System.currentTimeMillis() / 1000;
            Date field1 = Calendar.getInstance().getTime();
            double field2 = i + 0.1;

            myMetric4Rows.insert(tag1v, tag2v, ts, field1, field2);
        }

        Collection<TableRows> rows = Arrays.asList(myMetric3Rows, myMetric4Rows);

        // For performance reasons, the SDK is designed to be purely asynchronous.
        // The return value is a future object. If you want to immediately obtain
        // the result, you can call `future.get()`.
        CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.write(rows);

        Result<WriteOk, Err> result = future.get();

        if (result.isOk()) {
            LOG.info("Write result: {}", result.getOk());
        } else {
            LOG.error("Failed to write: {}", result.getErr());
        }

        List<TableRows> delete_pojos = Arrays.asList(myMetric3Rows.subRange(0, 5), myMetric4Rows.subRange(0, 5));
        Result<WriteOk, Err> deletes = greptimeDB.write(delete_pojos, WriteOp.Delete).get();

        if (deletes.isOk()) {
            LOG.info("Delete result: {}", result.getOk());
        } else {
            LOG.error("Failed to delete: {}", result.getErr());
        }
    }
}
