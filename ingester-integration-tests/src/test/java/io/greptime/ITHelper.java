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

import io.greptime.models.AuthInfo;
import io.greptime.options.GreptimeOptions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper utilities for integration tests.
 */
public final class ITHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ITHelper.class);

    private static final String DEFAULT_ENDPOINTS = "localhost:4001";
    private static final String DEFAULT_DATABASE = "public";
    private static final String DEFAULT_JDBC_URL = "jdbc:mysql://localhost:4002/public";

    private ITHelper() {}

    /**
     * Gets the gRPC endpoints from environment variable or default.
     */
    public static String getEndpoints() {
        return getEnvOrDefault("GREPTIMEDB_ENDPOINTS", DEFAULT_ENDPOINTS);
    }

    /**
     * Gets the database name from environment variable or default.
     */
    public static String getDatabase() {
        return getEnvOrDefault("GREPTIMEDB_DATABASE", DEFAULT_DATABASE);
    }

    /**
     * Gets the JDBC URL from environment variable or default.
     */
    public static String getJdbcUrl() {
        return getEnvOrDefault("GREPTIMEDB_JDBC_URL", DEFAULT_JDBC_URL);
    }

    /**
     * Creates a GreptimeDB client with configuration from environment variables.
     */
    public static GreptimeDB createClient() {
        String[] endpoints = getEndpoints().split(",");
        String database = getDatabase();

        GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
                .authInfo(AuthInfo.noAuthorization())
                .build();

        return GreptimeDB.create(opts);
    }

    /**
     * Creates a JDBC connection to GreptimeDB.
     */
    public static Connection getJdbcConnection() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("MySQL JDBC driver not found", e);
        }
        return DriverManager.getConnection(getJdbcUrl());
    }

    /**
     * Executes a SQL update statement (DDL/DML).
     */
    public static void executeUpdate(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            LOG.debug("Executing SQL: {}", sql);
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Queries the row count of a table.
     */
    public static int queryCount(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
            return 0;
        }
    }

    /**
     * Queries a single row by host and verifies it exists.
     * Returns the ResultSet for further verification.
     */
    public static ResultSet queryByHost(Connection conn, String tableName, String host) throws SQLException {
        String sql = String.format("SELECT * FROM %s WHERE host = '%s'", tableName, host);
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Verifies that a specific row exists with expected values.
     */
    public static void verifyRow(
            Connection conn, String tableName, String expectedHost, double expectedCpuUser, double expectedCpuSys)
            throws SQLException {
        String sql = String.format("SELECT host, cpu_user, cpu_sys FROM %s WHERE host = '%s'", tableName, expectedHost);
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("Row not found for host: " + expectedHost);
            }
            String actualHost = rs.getString("host");
            double actualCpuUser = rs.getDouble("cpu_user");
            double actualCpuSys = rs.getDouble("cpu_sys");

            if (!expectedHost.equals(actualHost)) {
                throw new AssertionError(
                        String.format("Host mismatch: expected=%s, actual=%s", expectedHost, actualHost));
            }
            if (Math.abs(expectedCpuUser - actualCpuUser) > 0.0001) {
                throw new AssertionError(
                        String.format("cpu_user mismatch: expected=%f, actual=%f", expectedCpuUser, actualCpuUser));
            }
            if (Math.abs(expectedCpuSys - actualCpuSys) > 0.0001) {
                throw new AssertionError(
                        String.format("cpu_sys mismatch: expected=%f, actual=%f", expectedCpuSys, actualCpuSys));
            }
        }
    }

    /**
     * Drops a table if it exists.
     */
    public static void dropTableIfExists(Connection conn, String tableName) {
        try {
            executeUpdate(conn, "DROP TABLE IF EXISTS " + tableName);
            LOG.debug("Dropped table: {}", tableName);
        } catch (SQLException e) {
            LOG.warn("Failed to drop table {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * Generates a unique table name with timestamp suffix.
     */
    public static String uniqueTableName(String prefix) {
        return prefix + "_" + System.currentTimeMillis();
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
