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
import io.greptime.models.Result;
import io.greptime.models.SemanticType;
import io.greptime.models.TableRows;
import io.greptime.models.TableSchema;
import io.greptime.models.WriteOk;
import io.greptime.options.WriteOptions;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import static io.greptime.models.DataType.Binary;
import static io.greptime.models.DataType.Bool;
import static io.greptime.models.DataType.Date;
import static io.greptime.models.DataType.DateTime;
import static io.greptime.models.DataType.Float32;
import static io.greptime.models.DataType.Float64;
import static io.greptime.models.DataType.Int16;
import static io.greptime.models.DataType.Int32;
import static io.greptime.models.DataType.Int64;
import static io.greptime.models.DataType.Int8;
import static io.greptime.models.DataType.TimestampMillisecond;
import static io.greptime.models.DataType.TimestampNanosecond;
import static io.greptime.models.DataType.TimestampSecond;
import static io.greptime.models.DataType.UInt16;
import static io.greptime.models.DataType.UInt32;
import static io.greptime.models.DataType.UInt64;
import static io.greptime.models.DataType.UInt8;
import static io.greptime.models.SemanticType.Field;
import static io.greptime.models.SemanticType.Tag;
import static io.greptime.models.SemanticType.Timestamp;

/**
 * @author jiachun.fjc
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
        TableSchema schema = TableSchema.newBuilder("test_table") //
                .addColumn("test_tag", Tag, DataType.String) //
                .addColumn("test_ts", Timestamp, DataType.Int64) //
                .addColumn("field1", Field, DataType.Int8) //
                .addColumn("field2", Field, DataType.Int16) //
                .addColumn("field3", Field, DataType.Int32) //
                .addColumn("field4", Field, DataType.Int64) //
                .addColumn("field5", Field, DataType.UInt8) //
                .addColumn("field6", Field, DataType.UInt16) //
                .addColumn("field7", Field, DataType.UInt32) //
                .addColumn("field8", Field, DataType.UInt64) //
                .addColumn("field9", Field, DataType.Float32) //
                .addColumn("field10", Field, DataType.Float64) //
                .addColumn("field11", Field, DataType.Bool) //
                .addColumn("field12", Field, DataType.Binary) //
                .addColumn("field13", Field, DataType.Date) //
                .addColumn("field14", Field, DataType.DateTime) //
                .addColumn("field15", Field, DataType.TimestampSecond) //
                .addColumn("field16", Field, DataType.TimestampMillisecond) //
                .addColumn("field17", Field, DataType.TimestampNanosecond) //
                .build();
        TableRows rows = TableRows.from(schema);
        long ts = System.currentTimeMillis();

        rows.insert("tag1", ts, 1, 2, 3, 4L, 5, 6, 7, 8L, 0.9F, 0.10D, true, new byte[0], 11, 12L, 13L, 14L, 15L);
        rows.insert("tag1", ts, 1, 2, 3, 4, 5, 6, 7, 8, 0.9, 0.10, false, new byte[0], 11, 12, 13, 14, 15);
        rows.insert("tag1", ts, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, false, new byte[] {0, 1}, 11, 12, 13, 14, 15);

        Endpoint addr = Endpoint.parse("127.0.0.1:8081");
        Database.GreptimeResponse response = Database.GreptimeResponse.newBuilder() //
                .setAffectedRows(Common.AffectedRows.newBuilder().setValue(3)) //
                .build();

        Mockito.when(this.routerClient.route()) //
                .thenReturn(Util.completedCf(addr));
        Mockito.when(this.routerClient.invoke(Mockito.eq(addr), Mockito.any(), Mockito.any())) //
                .thenReturn(Util.completedCf(response));

        Result<WriteOk, Err> res = this.writeClient.write(Collections.singleton(rows)).get();

        Assert.assertTrue(res.isOk());
        Assert.assertEquals(3, res.getOk().getSuccess());
    }
}
