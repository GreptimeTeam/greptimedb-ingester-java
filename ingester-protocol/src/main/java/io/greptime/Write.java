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

import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.WriteOk;
import io.greptime.models.Table;
import io.greptime.rpc.Context;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Write API: writes data in row format to the DB.
 *
 * @author jiachun.fjc
 */
public interface Write {

    /**
     * @see #write(Collection, WriteOp, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> write(Collection<Table> rows) {
        return write(rows, WriteOp.Insert, Context.newDefault());
    }

    /**
     * @see #write(Collection, WriteOp, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> write(Collection<Table> rows, WriteOp writeOp) {
        return write(rows, writeOp, Context.newDefault());
    }

    /**
     * Write multi tables multi rows data to database.
     *
     * @param rows rows with multi tables
     * @param writeOp write operation(insert or delete)
     * @param ctx invoke context
     * @return write result
     */
    CompletableFuture<Result<WriteOk, Err>> write(Collection<Table> rows, WriteOp writeOp, Context ctx);

    /**
     * @see #streamWriter(int, Context)
     */
    default StreamWriter<Table, WriteOk> streamWriter() {
        return streamWriter(-1);
    }

    /**
     * @see #streamWriter(int, Context)
     */
    default StreamWriter<Table, WriteOk> streamWriter(int maxPointsPerSecond) {
        return streamWriter(maxPointsPerSecond, Context.newDefault());
    }

    /**
     * Create a `Stream` to write `Table` data.
     *
     * @param maxPointsPerSecond The max number of points that can be written per second,
     *                           exceeding which may cause blockage.
     * @param ctx invoke context
     * @return a stream writer instance
     */
    StreamWriter<Table, WriteOk> streamWriter(int maxPointsPerSecond, Context ctx);
}
