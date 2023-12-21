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
import java.math.BigDecimal;

/**
 * @author jiachun.fjc
 */
@Metric(name = "my_metric1")
public class MyMetric1 {
    @Column(name = "tag1", tag = true, dataType = DataType.String)
    private String tag1;
    @Column(name = "tag2", tag = true, dataType = DataType.String)
    private String tag2;
    @Column(name = "tag3", tag = true, dataType = DataType.String)
    private String tag3;

    @Column(name = "ts", timestamp = true, dataType = DataType.TimestampMillisecond)
    private long ts;

    @Column(name = "field1", dataType = DataType.String)
    private String field1;
    @Column(name = "field2", dataType = DataType.Float64)
    private double field2;
    @Column(name = "field3", dataType = DataType.Decimal128)
    private BigDecimal field3;
    @Column(name = "field4", dataType = DataType.Int32)
    private int field4;

    public String getTag1() {
        return tag1;
    }

    public void setTag1(String tag1) {
        this.tag1 = tag1;
    }

    public String getTag2() {
        return tag2;
    }

    public void setTag2(String tag2) {
        this.tag2 = tag2;
    }

    public String getTag3() {
        return tag3;
    }

    public void setTag3(String tag3) {
        this.tag3 = tag3;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getField1() {
        return field1;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public double getField2() {
        return field2;
    }

    public void setField2(double field2) {
        this.field2 = field2;
    }

    public BigDecimal getField3() {
        return field3;
    }

    public void setField3(BigDecimal field3) {
        this.field3 = field3;
    }

    public int getField4() {
        return field4;
    }

    public void setField4(int field4) {
        this.field4 = field4;
    }
}
