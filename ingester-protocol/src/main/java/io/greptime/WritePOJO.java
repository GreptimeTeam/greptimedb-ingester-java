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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Write POJO API: writes data in POJO object format to the DB.
 *
 * @author jiachun.fjc
 */
public interface WritePOJO {
    /**
     * @see #writePOJOs(Collection, Context)
     */
    default CompletableFuture<Result<WriteOk, Err>> writePOJOs(Collection<List<?>> pojos) {
        return writePOJOs(pojos, Context.newDefault());
    }

    /**
     * Write multi tables multi rows data to database.
     *
     * @param pojos rows with multi tables
     * @param ctx invoke context
     * @return write result
     */
    CompletableFuture<Result<WriteOk, Err>> writePOJOs(Collection<List<?>> pojos, Context ctx);

    /**
     * @see #streamWriterPOJOs(int, Context)
     */
    default StreamWriter<List<?>, WriteOk> streamWriterPOJOs() {
        return streamWriterPOJOs(-1);
    }

    /**
     * @see #streamWriterPOJOs(int, Context)
     */
    default StreamWriter<List<?>, WriteOk> streamWriterPOJOs(int maxPointsPerSecond) {
        return streamWriterPOJOs(maxPointsPerSecond, Context.newDefault());
    }

    /**
     * Create a streaming for write.
     *
     * @param maxPointsPerSecond The max number of points that can be written per second,
     *                           exceeding which may cause blockage.
     * @param ctx invoke context
     * @return a stream writer instance
     */
    StreamWriter<List<?>, WriteOk> streamWriterPOJOs(int maxPointsPerSecond, Context ctx);
}
