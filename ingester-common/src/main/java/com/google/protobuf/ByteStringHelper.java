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

package com.google.protobuf;

/**
 * A {@code ByteString} helper, avoid some memory copying to improve performance.
 *
 *
 */
public class ByteStringHelper {

    /**
     * Wrap a byte array into a ByteString.
     */
    public static ByteString wrap(byte[] bs) {
        return ByteString.wrap(bs);
    }
}
