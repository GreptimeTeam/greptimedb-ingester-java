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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * @author jiachun.fjc
 */
public class QueryJDBC {

    public static void main(String[] args) throws Exception {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        // Inserts data first


        try (Connection conn = makeConnection()) {

        }
    }

    public static Connection makeConnection() throws IOException, ClassNotFoundException, SQLException {
        Properties prop = new Properties();
        prop.load(QueryJDBC.class.getResourceAsStream("/db-connection.properties"));

        String dbName = (String) prop.get("db.database-driver");

        String dbConnUrl = (String) prop.get("db.url");
        String dbUserName = (String) prop.get("db.username");
        String dbPassword = (String) prop.get("db.password");

        Class.forName(dbName);
        Connection dbConn = DriverManager.getConnection(dbConnUrl, dbUserName, dbPassword);

        return Objects.requireNonNull(dbConn, "Failed to make connection!");
    }
}
