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
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import io.greptime.v1.Common;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class TestUtil {

    public static Collection<Table> testTable(String tableName, int rowCount) {
        TableSchema tableSchema = TableSchema.newBuilder(tableName) //
                .addTag("host", DataType.String) //
                .addTimestamp("ts", DataType.TimestampMillisecond) //
                .addField("cpu", DataType.Float64) //
                .build();

        Table table = Table.from(tableSchema);
        for (int i = 0; i < rowCount; i++) {
            table.addRow("127.0.0.1", System.currentTimeMillis(), i);
        }
        return Collections.singleton(table);
    }

    public static BigDecimal getDecimal(Common.Decimal128 decimal128, int scale) {
        long lo = decimal128.getLo();
        BigInteger loValue = BigInteger.valueOf(lo & Long.MAX_VALUE);
        if (lo < 0) {
            loValue = loValue.add(BigInteger.valueOf(1).shiftLeft(63));
        }

        BigInteger unscaledValue = BigInteger.valueOf(decimal128.getHi());
        unscaledValue = unscaledValue.shiftLeft(64);
        unscaledValue = unscaledValue.add(loValue);

        return new BigDecimal(unscaledValue, scale);
    }
}
