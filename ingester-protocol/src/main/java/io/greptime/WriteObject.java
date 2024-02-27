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
import io.greptime.rpc.Context;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Write POJO object API: writes data in POJO object format to the DB.
 *
 * @author jiachun.fjc
 */
public interface WriteObject {
    /**
     * @see #writeObjects(Collection, WriteOp, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> writeObjects(List<?>... objects) {
        return writeObjects(Arrays.asList(objects));
    }
    /**
     * @see #writeObjects(Collection, WriteOp, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> writeObjects(Collection<List<?>> objects) {
        return writeObjects(objects, WriteOp.Insert, Context.newDefault());
    }

    /**
     * @see #writeObjects(Collection, WriteOp, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> writeObjects(Collection<List<?>> objects, WriteOp writeOp) {
        return writeObjects(objects, writeOp, Context.newDefault());
    }

    /**
     * Write multiple rows of data (which can belong to multiple tables) to the database at once.
     *
     * @param objects a collection of data to be written, classified by table
     * @param writeOp write operation(insert or delete)
     * @param ctx invoke context
     * @return write result
     */
    CompletableFuture<Result<WriteOk, Err>> writeObjects(Collection<List<?>> objects, WriteOp writeOp, Context ctx);

    /**
     * @see #objectsStreamWriter(int, Context)
     */
    default StreamWriter<List<?>, WriteOk> objectsStreamWriter() {
        return objectsStreamWriter(-1);
    }

    /**
     * @see #objectsStreamWriter(int, Context)
     */
    default StreamWriter<List<?>, WriteOk> objectsStreamWriter(int maxPointsPerSecond) {
        return objectsStreamWriter(maxPointsPerSecond, Context.newDefault());
    }

    /**
     * Create a `Stream` to continuously write data to the database, typically used in data import
     * scenarios. After completion, the stream needs to be closed(Call `StreamWriter#completed()`),
     * and the write result can be obtained from the database server.
     * <p>
     * It is important to note that each write operation can write a List of POJOs. However,
     * the POJO objects in the List must have the same type. If you need to write different types
     * of POJO objects, you can perform multiple write operations on the `Stream`, dividing them
     * into separate writes when you obtain the `Stream`.
     *
     * @param maxPointsPerSecond The max number of points that can be written per second,
     *                           exceeding which may cause blockage.
     * @param ctx invoke context
     * @return a stream writer instance
     */
    StreamWriter<List<?>, WriteOk> objectsStreamWriter(int maxPointsPerSecond, Context ctx);
}
