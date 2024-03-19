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

package io.greptime.models;

import io.greptime.WriteOp;
import java.util.Collection;
import java.util.Collections;

/**
 * @author jiachun.fjc
 */
public class WriteTables {
    private final Collection<Table> tables;
    private final WriteOp writeOp;

    public WriteTables(Table table, WriteOp writeOp) {
        this(Collections.singleton(table), writeOp);
    }

    public WriteTables(Collection<Table> tables, WriteOp writeOp) {
        this.tables = tables;
        this.writeOp = writeOp;
    }

    public Collection<Table> getTables() {
        return tables;
    }

    public WriteOp getWriteOp() {
        return writeOp;
    }
}
