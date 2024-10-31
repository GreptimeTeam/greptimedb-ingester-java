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

import io.greptime.common.Endpoint;
import io.greptime.models.DataType;
import io.greptime.models.Err;
import io.greptime.models.IntervalMonthDayNano;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.options.WriteOptions;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 */
@RunWith(value = MockitoJUnitRunner.class)
public class WriteClientTest {
    private WriteClient writeClient;

    @Mock
    private RouterClient routerClient;

    @Before
    public void before() {
        WriteOptions writeOpts = new WriteOptions();
        writeOpts.setAsyncPool(ForkJoinPool.commonPool());
        writeOpts.setRouterClient(this.routerClient);

        this.writeClient = new WriteClient();
        this.writeClient.init(writeOpts);
    }

    @After
    public void after() {
        this.writeClient.shutdownGracefully();
        this.routerClient.shutdownGracefully();
    }

    @Test
    public void testWriteSuccess() throws ExecutionException, InterruptedException {
        TableSchema schema = TableSchema.newBuilder("test_table")
                .addTag("test_tag", DataType.String)
                .addTimestamp("test_ts", DataType.TimestampMillisecond)
                .addField("field1", DataType.Int8)
                .addField("field2", DataType.Int16)
                .addField("field3", DataType.Int32)
                .addField("field4", DataType.Int64)
                .addField("field5", DataType.UInt8)
                .addField("field6", DataType.UInt16)
                .addField("field7", DataType.UInt32)
                .addField("field8", DataType.UInt64)
                .addField("field9", DataType.Float32)
                .addField("field10", DataType.Float64)
                .addField("field11", DataType.Bool)
                .addField("field12", DataType.Binary)
                .addField("field13", DataType.Date)
                .addField("field14", DataType.DateTime)
                .addField("field15", DataType.TimestampSecond)
                .addField("field16", DataType.TimestampMillisecond)
                .addField("field17", DataType.TimestampMicrosecond)
                .addField("field18", DataType.TimestampNanosecond)
                .addField("field19", DataType.TimeSecond)
                .addField("field20", DataType.TimeMilliSecond)
                .addField("field21", DataType.TimeMicroSecond)
                .addField("field22", DataType.TimeNanoSecond)
                .addField("field23", DataType.IntervalYearMonth)
                .addField("field24", DataType.IntervalDayTime)
                .addField("field25", DataType.IntervalMonthDayNano)
                .addField("field26", DataType.Decimal128)
                .addField("field27", DataType.Json)
                .build();
        Table table = Table.from(schema);
        long ts = System.currentTimeMillis();

        // spotless:off
        Object[] row1 = new Object[]{"tag1", ts, 1, 2, 3, 4L, 5, 6, 7, 8L, 0.9F, 0.10D, true, new byte[0], 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23, 24L, new IntervalMonthDayNano(1, 2, 3), BigDecimal.valueOf(123.456), "{\"a\": 1}"};
        Object[] row2 = new Object[]{"tag2", ts, 1, 2, 3, 4L, 5, 6, 7, 8L, 0.9F, 0.10D, true, new byte[0], 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23, 24L, new IntervalMonthDayNano(4, 5, 6), BigDecimal.valueOf(123.456), "{\"b\": 2}"};
        Object[] row3 = new Object[]{"tag3", ts, 1, 2, 3, 4L, 5, 6, 7, 8L, 0.9F, 0.10D, true, new byte[0], 13L, 14L, 15L, 16L, 17L, 18L, 19L, 20L, 21L, 22L, 23, 24L, new IntervalMonthDayNano(7, 8, 9), BigDecimal.valueOf(123.456), "{\"c\": 3}"};
        // spotless:on

        table.addRow(row1);
        table.addRow(row2);
        table.addRow(row3);

        Endpoint addr = Endpoint.parse("127.0.0.1:8081");
        Database.GreptimeResponse response = Database.GreptimeResponse.newBuilder()
                .setAffectedRows(Common.AffectedRows.newBuilder().setValue(3))
                .build();

        Mockito.when(this.routerClient.route()).thenReturn(Util.completedCf(addr));
        Mockito.when(this.routerClient.invoke(Mockito.eq(addr), Mockito.any(), Mockito.any()))
                .thenReturn(Util.completedCf(response));

        Result<WriteOk, Err> res = this.writeClient.write(table).get();

        Assert.assertTrue(res.isOk());
        Assert.assertEquals(3, res.getOk().getSuccess());
    }
}
