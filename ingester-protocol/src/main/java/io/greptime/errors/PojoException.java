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

package io.greptime.errors;

/**
 * Exception for pojo errors.
 */
public class PojoException extends RuntimeException {

    private static final long serialVersionUID = -1L;

    public PojoException(String message) {
        super(message);
    }

    public PojoException(Throwable cause) {
        super(cause);
    }
}
