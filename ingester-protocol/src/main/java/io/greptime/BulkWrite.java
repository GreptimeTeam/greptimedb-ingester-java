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

import io.greptime.models.TableSchema;
import io.greptime.rpc.Context;

/**
 * BulkWrite API: writes data to the database in bulk.
 */
public interface BulkWrite {

    /**
     * The default timeout in milliseconds for each message.
     */
    long DEFAULT_TIMEOUT_MS_PER_MESSAGE = 10000;

    /**
     * The default allocator init reservation bytes.
     */
    long DEFAULT_ALLOCATOR_INIT_RESERVATION = 0;

    /**
     * The default allocator max allocation bytes.
     */
    long DEFAULT_ALLOCATOR_MAX_ALLOCATION = 1024 * 1024 * 1024;

    /**
     * @see #bulkStreamWriter(TableSchema, long, long, Context)
     */
    default BulkStreamWriter bulkStreamWriter(TableSchema schema) {
        return bulkStreamWriter(
                schema,
                DEFAULT_ALLOCATOR_INIT_RESERVATION,
                DEFAULT_ALLOCATOR_MAX_ALLOCATION,
                DEFAULT_TIMEOUT_MS_PER_MESSAGE,
                Context.newDefault());
    }

    /**
     * @see #bulkStreamWriter(TableSchema, long, long, Context)
     */
    default BulkStreamWriter bulkStreamWriter(
            TableSchema schema, long allocatorInitReservation, long allocatorMaxAllocation) {
        return bulkStreamWriter(
                schema,
                allocatorInitReservation,
                allocatorMaxAllocation,
                DEFAULT_TIMEOUT_MS_PER_MESSAGE,
                Context.newDefault());
    }

    /**
     * Creates a bulk stream writer for efficiently writing data to the server.
     *
     * @param schema the schema of the table
     * @param allocatorInitReservation the initial space reservation (obtained from this allocator)
     * @param allocatorMaxAllocation the maximum amount of space the new child allocator can allocate
     * @param timeoutMsPerMessage the timeout in milliseconds for each message
     * @param ctx invoke context
     * @return a bulk stream writer instance
     */
    BulkStreamWriter bulkStreamWriter(
            TableSchema schema,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            long timeoutMsPerMessage,
            Context ctx);
}
