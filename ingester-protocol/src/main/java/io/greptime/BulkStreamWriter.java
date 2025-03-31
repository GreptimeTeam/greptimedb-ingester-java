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

import io.greptime.models.Table;
import java.util.concurrent.CompletableFuture;

/**
 * BulkStreamWriter is a specialized interface for efficiently writing data to the server in bulk operations.
 */
public interface BulkStreamWriter extends AutoCloseable {
    /**
     * Allocate a new table buffer.
     *
     * @return a new table buffer
     */
    Table allocateTableBuffer();

    /**
     * Writes currenttable data to the stream.
     *
     * @return a future that completes with true if the message was sent, false otherwise.
     */
    CompletableFuture<Boolean> writeNext();

    /**
     * Completes the bulk write operation by signaling the end of transmission
     * and waits for the server to finish processing the data. This method
     * must be called to ensure all data is properly written and to receive
     * any errors that may have occurred during the operation.
     */
    void completed() throws Exception;
}
