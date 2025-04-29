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

package io.greptime.models;

import io.greptime.common.Endpoint;

/**
 * Contains the write/query error value.
 */
public class Err {
    // error code from server
    private int code;
    // error message
    private Throwable error;
    // the server address where the error occurred
    private Endpoint errTo;

    /**
     * Gets the error code.
     *
     * @return the error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the error.
     *
     * @return the error
     */
    public Throwable getError() {
        return error;
    }

    /**
     * Gets the server address where the error occurred.
     *
     * @return the server address where the error occurred
     */
    public Endpoint getErrTo() {
        return errTo;
    }

    /**
     * Maps this error to a {@link Result}.
     *
     * @param <T> the type of the result
     * @return a {@link Result} containing this error
     */
    public <T> Result<T, Err> mapToResult() {
        return Result.err(this);
    }

    @Override
    public String toString() {
        return "Err{" + "code=" + code + ", error='" + error + '\'' + ", errTo=" + errTo + '}';
    }

    /**
     * Creates a new {@link Err} for write error.
     *
     * @param code the error code
     * @param error the error
     * @param errTo the server address where the error occurred
     * @return a new {@link Err} for write error
     */
    public static Err writeErr(int code, Throwable error, Endpoint errTo) {
        Err err = new Err();
        err.code = code;
        err.error = error;
        err.errTo = errTo;
        return err;
    }
}
