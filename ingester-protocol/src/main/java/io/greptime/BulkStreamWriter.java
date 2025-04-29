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
 * {@code BulkStreamWriter} is a specialized interface for efficiently writing data to the server in bulk operations.
 *
 * <p>Each {@code BulkStreamWriter} is associated with a {@code TableBufferRoot} instance that manages off-heap memory.
 * The workflow involves first obtaining the {@code TableBufferRoot} instance and populating it with data.
 * This data resides in off-heap memory and is only transmitted to the server when {@code writeNext()}
 * is called. Upon calling {@code writeNext()}, all data in the {@code TableBufferRoot} is sent as a batch,
 * and the {@code TableBufferRoot} is automatically cleared for the next set of data.
 *
 * <p>As a streaming interface, {@code BulkStreamWriter} allows you to repeat this cycle (populate data,
 * call {@code writeNext()}) multiple times for efficient batch processing. When all data has been
 * transmitted, you must call {@code completed()} to properly close the stream and ensure any server-side
 * errors are properly reported. Additionally, you should call the {@code close()} method to ensure all
 * related resources are properly released, though this happens automatically when using try-with-resources.
 *
 * <p>Example usage:
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
 *         // Complete the table; adding rows is no longer permitted.
 *         table.complete();
 *
 *         LOG.info("Prepare data, time cost: {}ms", System.currentTimeMillis() - start);
 *
 *         start = System.currentTimeMillis();
 *         CompletableFuture<Integer> future = bulkStreamWriter.writeNext();
 *         Integer result = future.get();
 *         LOG.info("Wrote rows: {}, time cost: {}ms", result, System.currentTimeMillis() - start);
 *     }
 *
 *     bulkStreamWriter.completed();
 * }
 * }</pre>
 */
public interface BulkStreamWriter extends AutoCloseable {
    /**
     * Returns the {@code TableBufferRoot} instance associated with this writer.
     * The {@code TableBufferRoot} provides direct access to the underlying memory
     * where table data is stored for efficient bulk operations.
     *
     * @param columnBufferSize the buffer size for each column
     *
     * @return a table buffer root
     */
    Table.TableBufferRoot tableBufferRoot(int columnBufferSize);

    /**
     * Writes current table data to the stream.
     *
     * @return a future that completes with the number of rows affected
     * @throws Exception if an error occurs
     */
    CompletableFuture<Integer> writeNext() throws Exception;

    /**
     * Completes the bulk write operation by signaling the end of transmission
     * and waits for the server to finish processing the data. This method
     * must be called to ensure all data is properly written and to receive
     * any errors that may have occurred during the operation.
     *
     * @throws Exception if an error occurs
     */
    void completed() throws Exception;

    /**
     * If the stream is not ready, calling {@code writeNext()} will block until the stream
     * becomes ready for writing, potentially using a busy-wait mechanism.
     *
     * @return true if the stream is ready to write data, false otherwise
     */
    default boolean isStreamReady() {
        return true;
    }
}
