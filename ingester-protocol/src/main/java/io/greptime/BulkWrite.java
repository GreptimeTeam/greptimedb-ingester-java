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
    long DEFAULT_TIMEOUT_MS_PER_MESSAGE = 60000;

    /**
     * The default allocator init reservation bytes.
     */
    long DEFAULT_ALLOCATOR_INIT_RESERVATION = 0;

    /**
     * The default allocator max allocation bytes.
     */
    long DEFAULT_ALLOCATOR_MAX_ALLOCATION = 1024 * 1024 * 1024;

    /**
     * The default max in-flight requests in the stream.
     *
     * This value should be determined based on the size of each request packet. A higher value means more in-flight requests,
     * which could potentially saturate network bandwidth or exceed the actual processing capacity of the database.
     */
    int DEFAULT_MAX_REQUESTS_IN_FLIGHT = 4;

    static class Config {
        private long allocatorInitReservation = DEFAULT_ALLOCATOR_INIT_RESERVATION;
        private long allocatorMaxAllocation = DEFAULT_ALLOCATOR_MAX_ALLOCATION;
        private long timeoutMsPerMessage = DEFAULT_TIMEOUT_MS_PER_MESSAGE;
        private int maxRequestsInFlight = DEFAULT_MAX_REQUESTS_IN_FLIGHT;

        private Config() {}

        public static Builder newBuilder() {
            return new Builder();
        }

        public long getAllocatorInitReservation() {
            return allocatorInitReservation;
        }

        public long getAllocatorMaxAllocation() {
            return allocatorMaxAllocation;
        }

        public long getTimeoutMsPerMessage() {
            return timeoutMsPerMessage;
        }

        public int getMaxRequestsInFlight() {
            return maxRequestsInFlight;
        }

        public static class Builder {
            private final Config config = new Config();

            /**
             * Set the initial space reservation for the allocator.
             *
             * @param allocatorInitReservation the initial space reservation
             * @return this builder
             */
            public Builder allocatorInitReservation(long allocatorInitReservation) {
                config.allocatorInitReservation = allocatorInitReservation;
                return this;
            }

            /**
             * Set the maximum amount of space the new child allocator can allocate.
             *
             * @param allocatorMaxAllocation the maximum amount of space
             * @return this builder
             */
            public Builder allocatorMaxAllocation(long allocatorMaxAllocation) {
                config.allocatorMaxAllocation = allocatorMaxAllocation;
                return this;
            }

            /**
             * Set the timeout in milliseconds for each message.
             *
             * @param timeoutMsPerMessage the timeout in milliseconds
             * @return this builder
             */
            public Builder timeoutMsPerMessage(long timeoutMsPerMessage) {
                config.timeoutMsPerMessage = timeoutMsPerMessage;
                return this;
            }

            /**
             * Set the max in-flight requests in the stream.
             *
             * This value should be determined based on the size of each request packet. A higher value means more in-flight requests,
             * which could potentially saturate network bandwidth or exceed the actual processing capacity of the database.
             *
             * @param maxRequestsInFlight the max in-flight requests
             * @return this builder
             */
            public Builder maxRequestsInFlight(int maxRequestsInFlight) {
                config.maxRequestsInFlight = maxRequestsInFlight;
                return this;
            }

            public Config build() {
                return config;
            }
        }
    }

    default BulkStreamWriter bulkStreamWriter(TableSchema schema) {
        return bulkStreamWriter(schema, Config.newBuilder().build());
    }

    default BulkStreamWriter bulkStreamWriter(TableSchema schema, Config config) {
        return bulkStreamWriter(schema, config, Context.newDefault());
    }

    default BulkStreamWriter bulkStreamWriter(TableSchema schema, Config config, Context ctx) {
        return bulkStreamWriter(
                schema,
                config.getAllocatorInitReservation(),
                config.getAllocatorMaxAllocation(),
                config.getTimeoutMsPerMessage(),
                config.getMaxRequestsInFlight(),
                ctx);
    }

    /**
     * Creates a bulk stream writer for efficiently writing data to the server.
     *
     * @param schema the schema of the table
     * @param allocatorInitReservation the initial space reservation (obtained from this allocator)
     * @param allocatorMaxAllocation the maximum amount of space the new child allocator can allocate
     * @param timeoutMsPerMessage the timeout in milliseconds for each message
     * @param maxRequestsInFlight the max in-flight requests in the stream
     * @param ctx invoke context
     * @return a bulk stream writer instance
     */
    BulkStreamWriter bulkStreamWriter(
            TableSchema schema,
            long allocatorInitReservation,
            long allocatorMaxAllocation,
            long timeoutMsPerMessage,
            int maxRequestsInFlight,
            Context ctx);
}
