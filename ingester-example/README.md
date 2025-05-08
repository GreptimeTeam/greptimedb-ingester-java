# GreptimeDB Java Ingester Examples

This module provides comprehensive examples demonstrating how to use the GreptimeDB Java ingester for efficient data ingestion. The examples showcase a range of APIs and approaches, from simple unary writes for basic use cases to high-performance bulk streaming for demanding production workloads. Each example includes detailed comments and best practices to help you choose the most appropriate ingestion method for your specific requirements.

- [Regular Write API](#regular-write-api)
  - [Batching Write](#batching-write)
    - [Performance Recommendations](#performance-recommendations)
    - [Examples](#examples)
  - [Streaming Write](#streaming-write)
    - [Examples](#examples-1)
- [Bulk Write API](#bulk-write-api)
  - [Examples](#examples-2)

## Regular Write API

The Regular Write API provides comprehensive methods for writing time-series data to GreptimeDB. This API supports both batching and streaming write operations with robust features including automatic error handling, configurable retry mechanisms, and rate limiting. It's designed to handle various ingestion patterns while maintaining optimal performance and reliability for your data pipeline.

### Batching Write

Batching write allows you to write data to one or more tables in a single operation, providing an efficient way to ingest time-series data.

The batch writing API offers a clean, straightforward approach to write data to GreptimeDB in a single request. It returns a `CompletableFuture<Result<WriteOk, Err>>` that completes when the write operation finishes. This asynchronous design enables high-performance data ingestion while providing clear success/failure information through the Result pattern.

This API is ideal for most use cases and should be your default choice when you're unsure which ingestion method to use. It balances simplicity, performance, and reliability for typical data ingestion scenarios.

#### Performance Recommendations

For optimal performance, we recommend batching your writes whenever possible:

- **Batch multiple rows**: Sending 500 rows in a single call rather than making 500 individual calls will significantly improve throughput and reduce network overhead.
- **Combine multiple tables**: This API allows you to write data to multiple tables in a single call, making it convenient to batch related data before sending it to the database.

These batching approaches can dramatically improve performance compared to making separate calls for each row or table, especially in high-throughput scenarios.

#### Examples

- [LowLevelApiWriteQuickStart.java](src/main/java/io/greptime/LowLevelApiWriteQuickStart.java)

  This example demonstrates how to use the low-level API to write data to GreptimeDB. It covers:
  * Defining table schemas with tags, timestamps, and fields
  * Writing multiple rows of data to different tables
  * Processing write results using the Result pattern
  * Deleting data using the `WriteOp.Delete` operation

- [HighLevelApiWriteQuickStart.java](src/main/java/io/greptime/HighLevelApiWriteQuickStart.java)

  This example demonstrates how to use the high-level API to write data to GreptimeDB. It covers:
  * Writing data using POJO objects with annotations
  * Handling multiple tables in a single write operation
  * Processing write results asynchronously
  * Deleting data using the `WriteOp.Delete` operation

### Streaming Write

The streaming write API establishes a persistent connection to GreptimeDB, enabling continuous data ingestion over time with built-in rate limiting. This approach provides a convenient way to write data from multiple tables through a single stream, prioritizing ease of use and consistent throughput.

This API is particularly well-suited for:
- Continuous data collection scenarios with moderate volume
- Applications that need to write to multiple tables through a single connection
- Cases where simplicity and convenience are more important than maximum throughput

#### Examples

- [LowLevelApiStreamWriteQuickStart.java](src/main/java/io/greptime/LowLevelApiStreamWriteQuickStart.java)

  This example demonstrates how to use the low-level API to write data to GreptimeDB using stream. It covers:
  * Defining table schemas with tags, timestamps, and fields
  * Writing multiple rows of data to different tables via streaming
  * Finalizing the stream and retrieving write results
  * Deleting data using the `WriteOp.Delete` operation

- [HighLevelApiStreamWriteQuickStart.java](src/main/java/io/greptime/HighLevelApiStreamWriteQuickStart.java)

  This example demonstrates how to use the high-level API to write data to GreptimeDB using stream. It covers:
  * Writing POJO objects directly to the stream
  * Managing multiple data types in a single stream
  * Finalizing the stream and processing results
  * Deleting data using the `WriteOp.Delete` operation

## Bulk Write API

The Bulk Write API provides a high-performance, memory-efficient mechanism for ingesting large volumes of time-series data into GreptimeDB. It leverages Apache Arrow's Flight protocol and off-heap memory management to achieve optimal throughput when writing batches of data.

Unlike regular streaming, this API allows continuous writing to only one table per stream, but can handle very large data volumes (up to 200MB per write). It features sophisticated adaptive flow control mechanisms that automatically adjust to your data throughput requirements.

The Bulk Write API achieves superior performance compared to the Regular Write API through:
- Off-heap memory management using Arrow buffers
- Efficient binary serialization with Apache Arrow
- Streaming data transfer with Arrow Flight protocol
- Optional compression to reduce network traffic
- Batched operations to amortize network overhead

This API is ideal for scenarios such as:
- Batch processing and data migrations
- Massive log data ingestion requiring high throughput
- Time-series data collection systems that need to process large volumes of data
- High-frequency sensor data ingestion
- Applications where performance and throughput are critical requirements

### Examples

- [BulkWriteApiQuickStart.java](src/main/java/io/greptime/BulkWriteApiQuickStart.java)

  This example demonstrates how to use the bulk write API to write large volumes of data to a single table with maximum efficiency. It covers:
  * Configuring the bulk writer for optimal performance
  * Writing large batches of data to a single table
  * Leveraging the adaptive flow control mechanisms
  * Processing write results asynchronously
