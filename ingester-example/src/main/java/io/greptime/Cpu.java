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

/**
 * @author jiachun.fjc
 */
@Metric(name = "cpu_metric")
public class Cpu {
    @Column(name = "host", tag = true, dataType = DataType.String)
    private String host;

    @Column(name = "ts", timestamp = true, dataType = DataType.TimestampMillisecond)
    private long ts;

    @Column(name = "cpu_user", dataType = DataType.Float64)
    private double cpuUser;

    @Column(name = "cpu_sys", dataType = DataType.Float64)
    private double cpuSys;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public double getCpuUser() {
        return cpuUser;
    }

    public void setCpuUser(double cpuUser) {
        this.cpuUser = cpuUser;
    }

    public double getCpuSys() {
        return cpuSys;
    }

    public void setCpuSys(double cpuSys) {
        this.cpuSys = cpuSys;
    }
}
