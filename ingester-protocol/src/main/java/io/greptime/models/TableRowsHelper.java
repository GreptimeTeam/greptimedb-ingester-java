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
public class TableRowsHelper {

    public static Database.GreptimeRequest toGreptimeRequest(TableRows rows, WriteOp writeOp, AuthInfo authInfo) {
        return toGreptimeRequest(Collections.singleton(rows.tableName()), Collections.singleton(rows), writeOp,
                authInfo);
    }

    public static Database.GreptimeRequest toGreptimeRequest(Collection<TableName> tableNames, //
            Collection<TableRows> rows, //
            WriteOp writeOp, //
            AuthInfo authInfo) {
        String dbName = null;
        for (TableName t : tableNames) {
            if (dbName == null) {
                dbName = t.getDatabaseName();
            } else if (!dbName.equals(t.getDatabaseName())) {
                String errMsg =
                        String.format("Write to multiple databases is not supported: %s, %s", dbName,
                                t.getDatabaseName());
                throw new IllegalArgumentException(errMsg);
            }
        }

        Common.RequestHeader.Builder headerBuilder = Common.RequestHeader.newBuilder();
        if (dbName != null) {
            headerBuilder.setDbname(dbName);
        }
        if (authInfo != null) {
            headerBuilder.setAuthorization(authInfo.into());
        }


        switch (writeOp) {
            case Insert:
                Database.RowInsertRequests.Builder insertBuilder = Database.RowInsertRequests.newBuilder();
                for (TableRows r : rows) {
                    insertBuilder.addInserts(r.intoRowInsertRequest());
                }
                return Database.GreptimeRequest.newBuilder() //
                        .setHeader(headerBuilder.build()) //
                        .setRowInserts(insertBuilder.build()) //
                        .build();
            case Delete:
                Database.RowDeleteRequests.Builder deleteBuilder = Database.RowDeleteRequests.newBuilder();
                for (TableRows r : rows) {
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

    private TableRowsHelper() {}
}
