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

import java.util.HashMap;
import java.util.Map;

/**
 * Common status code for public API.
 * <p>
 * Always follow <a href="https://github.com/GreptimeTeam/greptimedb/blob/main/src/common/error/src/status_code.rs">GreptimeDB's STATUS CODE</a>
 */
public enum Status {
    // ====== Begin of common status code ==============
    // Success.
    Success(0),

    // Unknown error.
    Unknown(1000),
    // Unsupported operation.
    Unsupported(1001),
    // Unexpected error, maybe there is a BUG.
    Unexpected(1002),
    // Internal server error.
    Internal(1003, true),
    // Invalid arguments.
    InvalidArguments(1004),
    // The task is cancelled.
    Cancelled(1005),
    // Illegal state.
    IllegalState(1006),
    // Caused by some error originated from external system.
    External(1007),
    // ====== End of common status code ================

    // ====== Begin of SQL related status code =========
    // SQL Syntax error.
    InvalidSyntax(2000),
    // ====== End of SQL related status code ===========

    // ====== Begin of query related status code =======
    // Fail to create a plan for the query.
    PlanQuery(3000),
    // The query engine fail to execute query.
    EngineExecuteQuery(3001),
    // ====== End of query related status code =========

    // ====== Begin of catalog related status code =====
    // Table already exists.
    TableAlreadyExists(4000),
    // Table not found.
    TableNotFound(4001),
    // Table column not found.
    TableColumnNotFound(4002),
    // Table column already exists.
    TableColumnExists(4003),
    // Database not found.
    DatabaseNotFound(4004),
    // Region not found.
    RegionNotFound(4005),
    // Region already exists.
    RegionAlreadyExists(4006),
    // Region is read-only in current state.
    RegionReadonly(4007),
    // Region is not in a proper state to handle specific request.
    RegionNotReady(4008),
    // Region is temporarily in busy state.
    RegionBusy(4009),
    // Table is temporarily unable to handle the request.
    TableUnavailable(4010),
    // Database already exists.
    DatabaseAlreadyExists(4011),
    // ====== End of catalog related status code =======

    // ====== Begin of storage related status code =====
    // Storage is temporarily unable to handle the request
    StorageUnavailable(5000, true),
    // Request is outdated, e.g., version mismatch.
    RequestOutdated(5001),
    // ====== End of storage related status code =======

    // ====== Begin of server related status code =====
    // Runtime resources exhausted, like creating threads failed.
    RuntimeResourcesExhausted(6000, true),
    // Rate limit exceeded
    RateLimited(6001),
    // ====== End of server related status code =======

    // ====== Begin of auth related status code =====
    // User not exist
    UserNotFound(7000),
    // Unsupported password type
    UnsupportedPasswordType(7001),
    // Username and password does not match
    UserPasswordMismatch(7002),
    // Not found http authorization header
    AuthHeaderNotFound(7003),
    // Invalid http authorization header
    InvalidAuthHeader(7004),
    // Illegal request to connect catalog-schema
    AccessDenied(7005),
    // User is not authorized to perform the operation
    PermissionDenied(7006);
    // ====== End of auth related status code =====

    private static final Map<Integer, Status> DICT = new HashMap<>();

    static {
        for (Status s : Status.values()) {
            DICT.put(s.getStatusCode(), s);
        }
    }

    private final int statusCode;
    private final boolean shouldRetry;

    Status(int statusCode) {
        this(statusCode, false);
    }

    Status(int statusCode, boolean shouldRetry) {
        this.statusCode = statusCode;
        this.shouldRetry = shouldRetry;
    }

    /**
     * Returns the status code.
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * Returns {@code true} if the status code is {@link #Success}.
     */
    public static boolean isSuccess(int statusCode) {
        return statusCode == Success.getStatusCode();
    }

    /**
     * Returns {@code true} if the status code represents a retry-needed error.
     */
    public boolean isShouldRetry() {
        return shouldRetry;
    }

    /**
     * Returns the {@link Status} for the specified status code.
     *
     * @param statusCode the status code
     * @return the {@link Status}
     */
    public static Status parse(int statusCode) {
        return DICT.get(statusCode);
    }
}
