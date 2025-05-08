# GreptimeDB Java Ingester

[![build](https://github.com/GreptimeTeam/greptimedb-ingester-java/actions/workflows/build.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-java/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.greptime/greptimedb-ingester.svg?label=maven%20central)](https://central.sonatype.com/search?q=io.greptime&name=ingester-all)

The GreptimeDB Ingester for Java is a lightweight, high-performance client designed for efficient time-series data ingestion. It leverages the gRPC protocol to provide a non-blocking, purely asynchronous API that delivers exceptional throughput while remaining easy to integrate into your applications.

This client offers multiple ingestion methods optimized for different performance requirements and use cases, allowing you to choose the approach that best fits your specific needs - from simple unary writes to high-throughput bulk streaming operations.

## Documentation
- [API Reference](https://javadoc.io/doc/io.greptime/ingester-protocol/latest/index.html)
- [Examples](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example)

## Features
- Writing data using
  - [Unary Write](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#unary-write)
  - [Streaming Write](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#streaming-write)
  - [Bulk Streaming Write](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#bulk-streaming-write)
- Management API client for managing
  - Health check
  - Authorizations

## High Level Architecture

```
+-----------------------------------+
|      Client Applications          |
|     +------------------+          |
|     | Application Code |          |
|     +------------------+          |
+-------------+---------------------+
              |
              v
+-------------+---------------------+
|           API Layer               |
|      +---------------+            |
|      |   GreptimeDB  |            |
|      +---------------+            |
|         /          \              |
|        v            v             |
| +-------------+  +-------------+  |        +------------------+
| |  BulkWrite  |  |    Write    |  |        |    Data Model    |
| |  Interface  |  |  Interface  |  |------->|                  |
| +-------------+  +-------------+  |        |  +------------+  |
+-------|----------------|----------+        |  |    Table   |  |
        |                |                   |  +------------+  |
        v                v                   |        |         |
+-------|----------------|----------+        |        v         |
|        Transport Layer            |        |  +------------+  |
| +-------------+  +-------------+  |        |  | TableSchema|  |
| |  BulkWrite  |  |    Write    |  |        |  +------------+  |
| |   Client    |  |    Client   |  |        +------------------+
| +-------------+  +-------------+  |
|     |    \          /    |        |
|     |     \        /     |        |
|     |      v      v      |        |
|     |  +-------------+   |        |
|     |  |RouterClient |   |        |
+-----|--+-------------|---+--------+
      |                |   |        |
      |                |   |        |
      v                v   v        |
+-----|----------------|---|--------+
|       Network Layer               |
| +-------------+  +-------------+  |
| | Arrow Flight|  | gRPC Client |  |
| |   Client    |  |             |  |
| +-------------+  +-------------+  |
|     |                |            |
+-----|----------------|------------+
      |                |
      v                v
   +-------------------------+
   |    GreptimeDB Server    |
   +-------------------------+
```

- **API Layer**: Provides high-level interfaces for client applications to interact with GreptimeDB
- **Data Model**: Defines the structure and organization of time series data with tables and schemas
- **Transport Layer**: Handles communication logistics, request routing, and client management
- **Network Layer**: Manages low-level protocol communications using Arrow Flight and gRPC

## How To Use
GreptimeDB Java Ingester is hosted in the Maven Central Repository.

To use it with Maven, simply add the following dependency to your project:

```XML
<dependency>
    <groupId>io.greptime</groupId>
    <artifactId>ingester-all</artifactId>
    <version>${latest_version}</version>
</dependency>
```

The latest version can be viewed [here](https://central.sonatype.com/search?q=io.greptime&name=ingester-all).


### Connect to database

The following code demonstrates how to connect to GreptimeDB with the simplest configuration.

```java
// GreptimeDB has a default database named "public" in the default catalog "greptime",
// we can use it as the test database
String database = "public";
// By default, GreptimeDB listens on port 4001 using the gRPC protocol.
// We can provide multiple endpoints that point to the same GreptimeDB cluster.
// The client will make calls to these endpoints based on a load balancing strategy.
// The client performs regular health checks and automatically routes requests to healthy nodes,
// providing fault tolerance and improved reliability for your application.
String[] endpoints = {"127.0.0.1:4001"};
// Sets authentication information.
AuthInfo authInfo = new AuthInfo("username", "password");
GreptimeOptions opts = GreptimeOptions.newBuilder(endpoints, database)
        // If the database does not require authentication, we can use `AuthInfo.noAuthorization()` as the parameter.
        .authInfo(authInfo)
        // Enable secure connection if your server is secured by TLS
        //.tlsOptions(new TlsOptions())
        // A good start ^_^
        .build();

GreptimeDB client = GreptimeDB.create(opts);
```

For customizing the connection options, please refer to [API Documentation](https://javadoc.io/doc/io.greptime/ingester-protocol/latest/index.html).
Please pay attention to the accompanying comments for each option, as they provide detailed explanations of their respective roles.

