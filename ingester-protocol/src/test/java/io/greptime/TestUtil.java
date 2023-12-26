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
import io.greptime.models.SemanticType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import java.util.Collection;
import java.util.Collections;

/**
 * @author jiachun.fjc
 */
public class TestUtil {

    public static Collection<Table> testTable(String tableName, int rowCount) {
        TableSchema tableSchema = TableSchema.newBuilder(tableName) //
                .addColumn("host", SemanticType.Tag, DataType.String) //
                .addColumn("ts", SemanticType.Timestamp, DataType.TimestampMillisecond) //
                .addColumn("cpu", SemanticType.Field, DataType.Float64) //
                .build();

        Table table = Table.from(tableSchema);
        for (int i = 0; i < rowCount; i++) {
            table.addRow("127.0.0.1", System.currentTimeMillis(), i);
        }
        return Collections.singleton(table);
    }
}
