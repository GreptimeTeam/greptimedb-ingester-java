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
 * `BulkStreamWriter` is a specialized interface for efficiently writing data to the server in bulk operations.
 *
 * Each `BulkStreamWriter` is associated with a `TableBufferRoot` instance that manages off-heap memory.
 * The workflow involves first obtaining the `TableBufferRoot` instance and populating it with data.
 * This data resides in off-heap memory and is only transmitted to the server when `writeNext()`
 * is called. Upon calling `writeNext()`, all data in the `TableBufferRoot` is sent as a batch,
 * and the `TableBufferRoot` is automatically cleared for the next set of data.
 *
 * As a streaming interface, `BulkStreamWriter` allows you to repeat this cycle (populate data,
 * call `writeNext()`) multiple times for efficient batch processing. When all data has been
 * transmitted, you must call `completed()` to properly close the stream and ensure any server-side
 * errors are properly reported. Additionally, you should call the `close()` method to ensure all
 * related resources are properly released, though this happens automatically when using try-with-resources.
 *
 * Example usage:
 * <pre>{@code
 * try (BulkStreamWriter bulkStreamWriter = greptimeDB.bulkStreamWriter(schema)) { // auto close in try-with-resources
 *     // Write 1000 times, each time write 100000 rows
 *     for (int i = 0; i < 1000; i++) {
 *         long start = System.currentTimeMillis();
 *         Table.TableBufferRoot table = bulkStreamWriter.tableBufferRoot();
 *         for (int j = 0; j < 100000; j++) {
 *             // with 100000 cardinality
 *             Object[] row = generateOneRow(100000);
 *             table.addRow(row);
 *         }
 *         table.complete();
 *         LOG.info("Prepare data, time cost: {}ms", System.currentTimeMillis() - start);
 *
 *         start = System.currentTimeMillis();
 *         CompletableFuture<Boolean> future = bulkStreamWriter.writeNext();
 *         Boolean result = future.get();
 *         LOG.info("Write result: {}, time cost: {}ms", result, System.currentTimeMillis() - start);
 *     }
 *
 *     bulkStreamWriter.completed();
 * }</pre>
 */
public interface BulkStreamWriter extends AutoCloseable {
    /**
     * Returns the `TableBufferRoot` instance associated with this writer.
     * The `TableBufferRoot` provides direct access to the underlying memory
     * where table data is stored for efficient bulk operations.
     *
     * @see Table.TableBufferRoot
     *
     * @return a table buffer root
     */
    Table.TableBufferRoot tableBufferRoot();

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
