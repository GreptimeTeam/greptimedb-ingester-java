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

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.nio.charset.StandardCharsets;

/**
 * Metadata classes used for request and response handling.
 */
public interface Metadata {
    // Gson's instances are Thread-safe we can reuse them freely across multiple threads.
    Gson GSON = new Gson();

    /**
     * Metadata included in requests to the server.
     */
    static class RequestMetadata {
        @SerializedName("request_id")
        private long requestId;

        RequestMetadata() {}

        /**
         * Creates a new RequestMetadata with the given request ID.
         *
         * @param requestId unique identifier for the request
         */
        public RequestMetadata(long requestId) {
            this.requestId = requestId;
        }

        /**
         * Converts this metadata to a JSON byte array using UTF-8 encoding.
         *
         * @return byte array containing the JSON representation
         */
        public byte[] toJsonBytesUtf8() {
            return GSON.toJson(this).getBytes(StandardCharsets.UTF_8);
        }

        public long getRequestId() {
            return requestId;
        }

        public void setRequestId(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public String toString() {
            return "RequestMetadata{requestId=" + requestId + '}';
        }
    }

    /**
     * Metadata included in responses from the server.
     */
    static class ResponseMetadata {
        @SerializedName("request_id")
        private long requestId;

        @SerializedName("affected_rows")
        private int affectedRows;

        /**
         * Creates a ResponseMetadata object from JSON string.
         *
         * @param json JSON string to parse
         * @return deserialized ResponseMetadata object
         */
        public static ResponseMetadata fromJson(String json) {
            return GSON.fromJson(json, ResponseMetadata.class);
        }

        public long getRequestId() {
            return requestId;
        }

        public void setRequestId(long requestId) {
            this.requestId = requestId;
        }

        public int getAffectedRows() {
            return affectedRows;
        }

        public void setAffectedRows(int affectedRows) {
            this.affectedRows = affectedRows;
        }

        @Override
        public String toString() {
            return "ResponseMetadata{requestId=" + requestId + ", affectedRows=" + affectedRows + '}';
        }
    }
}
