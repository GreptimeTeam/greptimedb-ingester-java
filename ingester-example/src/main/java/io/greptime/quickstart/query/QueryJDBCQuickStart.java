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

package io.greptime.quickstart.query;

import io.greptime.GreptimeDB;
import io.greptime.metric.Cpu;
import io.greptime.quickstart.TestConnector;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class QueryJDBCQuickStart {

    private static final Logger LOG = LoggerFactory.getLogger(QueryJDBCQuickStart.class);

    public static void main(String[] args) throws Exception {
        GreptimeDB greptimeDB = TestConnector.connectToDefaultDB();

        // Inserts data for query
        insertData(greptimeDB);

        try (Connection conn = getConnection()) {
            Statement statement = conn.createStatement();

            // DESC table;
            ResultSet rs = statement.executeQuery("DESC cpu_metric");
            LOG.info("Column | Type | Key | Null | Default | Semantic Type ");
            while (rs.next()) {
                LOG.info(
                        "{} | {} | {} | {} | {} | {}",
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getString(5),
                        rs.getString(6));
            }

            // SELECT COUNT(*) FROM cpu_metric;
            rs = statement.executeQuery("SELECT COUNT(*) FROM cpu_metric");
            while (rs.next()) {
                LOG.info("Count: {}", rs.getInt(1));
            }

            // SELECT * FROM cpu_metric ORDER BY ts DESC LIMIT 5;
            rs = statement.executeQuery("SELECT * FROM cpu_metric ORDER BY ts DESC LIMIT 5");
            LOG.info("host | ts | cpu_user | cpu_sys");
            while (rs.next()) {
                LOG.info(
                        "{} | {} | {} | {}",
                        rs.getString("host"),
                        rs.getTimestamp("ts"),
                        rs.getDouble("cpu_user"),
                        rs.getDouble("cpu_sys"));
            }
        }
    }

    public static Connection getConnection() throws IOException, ClassNotFoundException, SQLException {
        Properties prop = new Properties();
        prop.load(QueryJDBCQuickStart.class.getResourceAsStream("/db-connection.properties"));

        String dbName = (String) prop.get("db.database-driver");

        String dbConnUrl = (String) prop.get("db.url");
        String dbUserName = (String) prop.get("db.username");
        String dbPassword = (String) prop.get("db.password");

        Class.forName(dbName);
        Connection dbConn = DriverManager.getConnection(dbConnUrl, dbUserName, dbPassword);

        return Objects.requireNonNull(dbConn, "Failed to make connection!");
    }

    public static void insertData(GreptimeDB greptimeDB) throws ExecutionException, InterruptedException {
        List<Cpu> cpus = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Cpu c = new Cpu();
            c.setHost("127.0.0." + i);
            c.setTs(System.currentTimeMillis());
            c.setCpuUser(i + 0.1);
            c.setCpuSys(i + 0.12);
            cpus.add(c);
        }
        greptimeDB.writeObjects(cpus).get();
    }
}
