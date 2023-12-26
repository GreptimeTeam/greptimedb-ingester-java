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

import io.greptime.WriteOp;
import io.greptime.v1.Common;
import io.greptime.v1.Database;
import java.util.Collection;
import java.util.Collections;

/**
 * @author jiachun.fjc
 */
public class TableHelper {

    public static Database.GreptimeRequest toGreptimeRequest(Table rows, //
            WriteOp writeOp, //
            String database, //
            AuthInfo authInfo) {
        return toGreptimeRequest(Collections.singleton(rows), writeOp, database, authInfo);
    }

    public static Database.GreptimeRequest toGreptimeRequest(Collection<Table> rows, //
            WriteOp writeOp, //
            String database, //
            AuthInfo authInfo) {
        Common.RequestHeader.Builder headerBuilder = Common.RequestHeader.newBuilder();
        if (database != null) {
            headerBuilder.setDbname(database);
        }
        if (authInfo != null) {
            headerBuilder.setAuthorization(authInfo.into());
        }

        switch (writeOp) {
            case Insert:
                Database.RowInsertRequests.Builder insertBuilder = Database.RowInsertRequests.newBuilder();
                for (Table r : rows) {
                    insertBuilder.addInserts(r.intoRowInsertRequest());
                }
                return Database.GreptimeRequest.newBuilder() //
                        .setHeader(headerBuilder.build()) //
                        .setRowInserts(insertBuilder.build()) //
                        .build();
            case Delete:
                Database.RowDeleteRequests.Builder deleteBuilder = Database.RowDeleteRequests.newBuilder();
                for (Table r : rows) {
                    deleteBuilder.addDeletes(r.intoRowDeleteRequest());
                }
                return Database.GreptimeRequest.newBuilder() //
                        .setHeader(headerBuilder.build()) //
                        .setRowDeletes(deleteBuilder.build()) //
                        .build();
            default:
                throw new IllegalArgumentException("Unsupported write operation: " + writeOp);
        }
    }

    private TableHelper() {}
}
