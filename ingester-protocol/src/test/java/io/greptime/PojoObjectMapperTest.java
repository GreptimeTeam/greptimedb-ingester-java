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

import io.greptime.models.Column;
import io.greptime.models.DataType;
import io.greptime.models.Metric;
import io.greptime.models.Table;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author jiachun.fjc
 */
public class PojoObjectMapperTest {

    @Test
    public void testToTable() {
        List<Pojo1Test> pojos1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Pojo1Test pojo1 = createNewPojo1Test();
            pojos1.add(pojo1);
        }
        Table tp1 = new CachedPojoObjectMapper().mapToTable(pojos1);
        Assert.assertEquals("pojo1", tp1.tableName());
        Assert.assertEquals(50, tp1.pointCount());


        List<Pojo2Test> pojos2 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Pojo2Test pojo2 = createNewPojo2Test();
            pojos2.add(pojo2);
        }
        Table tp2 = new CachedPojoObjectMapper().mapToTable(pojos2);
        Assert.assertEquals("pojo2", tp2.tableName());
        Assert.assertEquals(30, tp2.pointCount());
    }

    static Pojo1Test createNewPojo1Test() {
        Random r = new Random();
        Pojo1Test pojo = new Pojo1Test();
        pojo.a = "abcdedfghijklmnopqrstuvwxyz";
        pojo.b = 1;
        pojo.c = r.nextLong();
        pojo.d = r.nextDouble();
        pojo.ts = System.currentTimeMillis() / 1000;
        return pojo;
    }

    static Pojo2Test createNewPojo2Test() {
        Pojo2Test pojo = new Pojo2Test();
        pojo.name = "pojo2";
        pojo.a = "a";
        pojo.ts = System.currentTimeMillis();
        return pojo;
    }
}


@Metric(name = "pojo1")
class Pojo1Test {
    @Column(name = "a", dataType = DataType.String, tag = true)
    String a;
    @Column(name = "b", dataType = DataType.Int8)
    int b;
    @Column(name = "c", dataType = DataType.Int64)
    long c;
    @Column(name = "d", dataType = DataType.Float64)
    double d;
    @Column(name = "ts", dataType = DataType.TimestampMillisecond, timestamp = true)
    long ts;
}


@Metric(name = "pojo2")
class Pojo2Test {
    @Column(name = "pojo2", dataType = DataType.String, tag = true)
    String name;
    @Column(name = "a", dataType = DataType.String)
    String a;
    @Column(name = "ts", dataType = DataType.TimestampMillisecond, timestamp = true)
    long ts;
}
