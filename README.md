# GreptimeDB Java Ingester

[![build](https://github.com/GreptimeTeam/greptimedb-ingester-java/actions/workflows/build.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-ingester-java/actions/workflows/build.yml)
![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)
[![Maven Central](https://img.shields.io/maven-central/v/io.greptime/greptimedb-ingester.svg?label=maven%20central)](https://central.sonatype.com/search?q=io.greptime&name=ingester-all)

The GreptimeDB Ingester for Java is a lightweight, high-performance client designed for efficient time-series data ingestion. It leverages the gRPC protocol to provide a non-blocking, purely asynchronous API that delivers exceptional throughput while maintaining seamless integration with your applications.

This client offers multiple ingestion methods optimized for various performance requirements and use cases. You can select the approach that best suits your specific needs—whether you require simple unary writes for low-latency operations or high-throughput bulk streaming for maximum efficiency when handling large volumes of time-series data.

## Documentation
- [API Reference](https://javadoc.io/doc/io.greptime/ingester-protocol/latest/index.html)
- [Examples](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example)

## Features
- Writing data using
  - [Regular Write API](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#regular-write-api)
    - [Batching Write](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#batching-write)
    - [Streaming Write](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#streaming-write)
  - [Bulk Write API](https://github.com/GreptimeTeam/greptimedb-ingester-java/tree/main/ingester-example#bulk-write-api)
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

- [Installation](#installation)
- [Client Initialization](#client-initialization)
- [Writing Data](#writing-data)
  - [Creating and Writing Tables](#creating-and-writing-tables)
    - [TableSchema](#tableschema)
    - [Column Types](#column-types)
    - [Table](#table)
- [Write Operations](#write-operations)
- [Streaming Write](#streaming-write)
- [Bulk Write](#bulk-write)
  - [Configuration](#configuration)
- [Resource Management](#resource-management)
- [Performance Tuning](#performance-tuning)
  - [Compression Options](#compression-options)
  - [Write Operation Comparison](#write-operation-comparison)
  - [Buffer Size Optimization](#buffer-size-optimization)

### Installation

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

### Client Initialization

The entry point to the GreptimeDB Ingester Java client is the `GreptimeDB` class. You create a client instance by calling the static create method with appropriate configuration options.

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

// Initialize the client
GreptimeDB client = GreptimeDB.create(opts);
```

### Writing Data

The ingester provides a unified approach for writing data to GreptimeDB through the `Table` abstraction. All data writing operations, including high-level APIs, are built on top of this fundamental structure. To write data, you create a `Table` with your time series data and write it to the database.

#### Creating and Writing Tables

Define a table schema and create a table:

```java
// Create a table schema
TableSchema schema = TableSchema.newBuilder("metrics")
    .addTag("host", DataType.String)
    .addTag("region", DataType.String)
    .addField("cpu_util", DataType.Float64)
    .addField("memory_util", DataType.Float64)
    .addTimestamp("ts", DataType.TimestampMillisecond)
    .build();

// Create a table from the schema
Table table = Table.from(schema);

// Add rows to the table
// The values must be provided in the same order as defined in the schema
// In this case: addRow(host, region, cpu_util, memory_util, ts)
table.addRow("host1", "us-west-1", 0.42, 0.78, System.currentTimeMillis());
table.addRow("host2", "us-west-2", 0.46, 0.66, System.currentTimeMillis());
// Add more rows
// ..

// Complete the table to make it immutable. This finalizes the table for writing.
// If users forget to call this method, it will automatically be called internally
// before the table data is written.
table.complete();

// Write the table to the database
CompletableFuture<Result<WriteOk, Err>> future = client.write(table);
```

##### TableSchema

The `TableSchema` defines the structure for writing data to GreptimeDB. It includes information about column names, semantic types, and data types.

##### Column Types

In GreptimeDB, columns are categorized into three semantic types:

- **Tag**: Columns used for filtering and grouping data
- **Field**: Columns that store the actual measurement values
- **Timestamp**: A special column that represents the time dimension

The timestamp column typically represents when data was sampled or when logs/events occurred. GreptimeDB identifies this column using a TIME INDEX constraint, which is why it's often referred to as the TIME INDEX column. If your schema contains multiple timestamp-type columns, only one can be designated as the TIME INDEX, while others must be defined as Field columns.

##### Table

The `Table` interface represents data that can be written to GreptimeDB. It provides methods for adding rows and manipulating the data. Essentially, `Table` temporarily stores data in memory, allowing you to accumulate multiple rows for batch processing before sending them to the database, which significantly improves write efficiency compared to writing individual rows.

A table goes through several distinct lifecycle stages:

1. **Creation**: Initialize a table from a schema using `Table.from(schema)`
2. **Data Addition**: Populate the table with rows using `addRow()` method
3. **Completion**: Finalize the table with `complete()` when all rows have been added
4. **Writing**: Send the completed table to the database

Important considerations:
- Tables are not thread-safe and should be accessed from a single thread
- Tables cannot be reused after writing - create a new instance for each write operation
- The associated `TableSchema` is immutable and can be safely reused across multiple operations

### Write Operations

You can also provide a custom context for more control:

```java
Context ctx = Context.newDefault();
// Add a hint to make the database create a table with the specified TTL (time-to-live)
ctx = ctx.withHint("ttl", "3d");
// Set the compression algorithm to Zstd.
ctx = ctx.withCompression(Compression.Zstd)
// Use the ctx when writing data to GreptimeDB
CompletableFuture<Result<WriteOk, Err>> future = client.write(Arrays.asList(table1, table2), WriteOp.Insert, ctx);
```

### Streaming Write

The streaming write API establishes a persistent connection to GreptimeDB, enabling continuous data ingestion over time with built-in rate limiting. This approach provides a convenient way to write data from multiple tables through a single stream, prioritizing ease of use and consistent throughput.

```java
// Create a stream writer
StreamWriter<Table, WriteOk> writer = client.streamWriter();

// Write multiple tables
writer.write(table1)
      .write(table2);

// Complete the stream and get the result
CompletableFuture<WriteOk> result = writer.completed();
```

You can also set a rate limit for stream writing:

```java
// Limit to 1000 points per second
StreamWriter<Table, WriteOk> writer = client.streamWriter(1000);
```

### Bulk Write

The Bulk Write API provides a high-performance, memory-efficient mechanism for ingesting large volumes of time-series data into GreptimeDB. It leverages off-heap memory management to achieve optimal throughput when writing batches of data.

This API supports writing to one table per stream and handles large data volumes (up to 200MB per write) with adaptive flow control. Performance advantages include:
- Off-heap memory management with Arrow buffers
- Efficient binary serialization and data transfer
- Optional compression
- Batched operations

This approach is particularly well-suited for:
- Large-scale batch processing and data migrations
- High-throughput log and sensor data ingestion
- Time-series applications with demanding performance requirements
- Systems processing high-frequency data collection

Here's a typical pattern for using the Bulk Write API:

```java
// Create a BulkStreamWriter with the table schema
try (BulkStreamWriter writer = greptimeDB.bulkStreamWriter(schema)) {
    // Write multiple batches
    for (int batch = 0; batch < batchCount; batch++) {
        // Get a TableBufferRoot for this batch
        Table.TableBufferRoot table = writer.tableBufferRoot(1000); // column buffer size
        
        // Add rows to the batch
        for (int row = 0; row < rowsPerBatch; row++) {
            Object[] rowData = generateRow(batch, row);
            table.addRow(rowData);
        }
        
        // Complete the table to prepare for transmission
        table.complete();
        
        // Send the batch and get a future for completion
        CompletableFuture<Integer> future = writer.writeNext();
        
        // Wait for the batch to be processed (optional)
        Integer affectedRows = future.get();
        
        System.out.println("Batch " + batch + " wrote " + affectedRows + " rows");
    }
    
    // Signal completion of the stream
    writer.completed();
}
```

#### Configuration

The Bulk Write API can be configured with several options to optimize performance:

```java
BulkWrite.Config cfg = BulkWrite.Config.newBuilder()
        .allocatorInitReservation(64 * 1024 * 1024L) // Customize memory allocation: 64MB initial reservation
        .allocatorMaxAllocation(4 * 1024 * 1024 * 1024L) // Customize memory allocation: 4GB max allocation
        .timeoutMsPerMessage(60 * 1000) // 60 seconds timeout per request
        .maxRequestsInFlight(8) // Concurrency Control: Configure with 10 maximum in-flight requests
        .build();
// Enable Zstd compression
Context ctx = Context.newDefault().withCompression(Compression.Zstd);

BulkStreamWriter writer = greptimeDB.bulkStreamWriter(schema, cfg, ctx);
```

### Resource Management

It's important to properly shut down the client when you're finished using it:

```java
// Gracefully shut down the client
client.shutdownGracefully();
```

### Performance Tuning

#### Compression Options

The GreptimeDB Ingester Java client supports various compression algorithms to reduce network bandwidth and improve throughput.

```java
// Set the compression algorithm to Zstd
Context ctx = Context.newDefault().withCompression(Compression.Zstd);
```

#### Write Operation Comparison

Understanding the performance characteristics of different write methods is crucial for optimizing data ingestion.

| Write Method | API | Throughput | Latency | Memory Efficiency | CPU Utilization |
|--------------|-----|------------|---------|-------------------|-----------------|
| Regular Write | `write(tables)` | Better | Good | High | Higher |
| Stream Write | `streamWriter()` | Moderate | Good | Moderate | Moderate |
| Bulk Write | `bulkStreamWriter()` | Best | Higher | Best | Moderate |


| Write Method | API | Best For | Limitations |
|-------------|-----|----------|-------------|
| Regular Write | `write(tables)` | Simple applications, low latency requirements | Lower throughput for large volumes |
| Stream Write | `streamWriter()` | Continuous data streams, moderate throughput | More complex to use than regular writes |
| Bulk Write | `bulkStreamWriter()` | Maximum throughput, large batch operations | Higher latency, more resource-intensive |

#### Buffer Size Optimization

When using `BulkStreamWriter`, you can configure the column buffer size:

```java
// Get the table buffer with a specific column buffer size
Table.TableBufferRoot table = bulkStreamWriter.tableBufferRoot(columnBufferSize);
```

This option can significantly improve the speed of data conversion to the underlying format. For optimal performance, we recommend setting the column buffer size to 1024 or larger, depending on your specific workload characteristics and available memory.

### Build Requirements

- Java 8+
- Maven 3.6+

### Contributing

We welcome contributions to the GreptimeDB Ingester Java client! Here's how you can contribute:

1. Fork the repository on GitHub
2. Create a feature branch for your changes
3. Make your changes, ensuring they follow the project's code style
4. Add appropriate tests for your changes
5. Submit a pull request to the main repository

When submitting a pull request, please ensure that:

- All tests pass successfully
- Code formatting is correct (run `mvn spotless:check`, this requires Java 17+)
- Documentation is updated to reflect your changes if necessary

Thank you for helping improve the GreptimeDB Ingester Java client! Your contributions are greatly appreciated.
