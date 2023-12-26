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
import java.util.Date;

/**
 * @author jiachun.fjc
 */
@Metric(name = "my_metric2")
public class MyMetric2 {
    @Column(name = "tag1", tag = true, dataType = DataType.String)
    private String tag1;
    @Column(name = "tag2", tag = true, dataType = DataType.String)
    private String tag2;

    @Column(name = "ts", timestamp = true, dataType = DataType.TimestampSecond)
    private long ts;

    @Column(name = "field1", dataType = DataType.Date)
    private Date field1;
    @Column(name = "field2", dataType = DataType.Float64)
    private double field2;

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

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public Date getField1() {
        return field1;
    }

    public void setField1(Date field1) {
        this.field1 = field1;
    }

    public double getField2() {
        return field2;
    }

    public void setField2(double field2) {
        this.field2 = field2;
    }
}
