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

import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * BulkStreamWriter is a specialized class for efficiently writing data to the server in bulk operations.
 *
 * It encapsulates two key components:
 * 1. A ClientStreamListener that manages and controls the data upload process
 * 2. A VectorSchemaRoot that contains the structured data to be transmitted
 *
 * This writer handles the serialization and transfer of Arrow-formatted data in an optimized manner,
 * providing a streamlined interface for bulk write operations.
 */
public class BulkStreamWriter implements AutoCloseable {
    private final ClientStreamListener listener;
    private final VectorSchemaRoot root;

    public BulkStreamWriter(ClientStreamListener listener, VectorSchemaRoot root) {
        this.listener = listener;
        this.root = root;
    }

    /**
     * Get the listener of the bulk stream.
     *
     * ClientStreamListener an interface to control uploading data.
     *
     * @return listener
     */
    public ClientStreamListener getListener() {
        return this.listener;
    }

    /**
     * Get the root of the bulk stream.
     *
     * VectorSchemaRoot the root containing data.
     *
     * @return root
     */
    public VectorSchemaRoot getRoot() {
        return this.root;
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(this.root);
    }
}
