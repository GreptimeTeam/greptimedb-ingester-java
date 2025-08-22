# GreptimeOptions Configuration Guide

## Overview

`GreptimeOptions` is the main configuration class for the GreptimeDB Java client, used to configure client connections, write options, RPC settings, and various other parameters. This document provides detailed information about all configuration options, their usage, and default values.

## Basic Usage

```java
// Create basic configuration
GreptimeOptions options = GreptimeOptions.newBuilder("127.0.0.1:4001", "public")
    .build();

// Create configuration with multiple endpoints
GreptimeOptions options = GreptimeOptions.newBuilder(
    new String[]{"127.0.0.1:4001", "127.0.0.1:4002"}, "public")
    .build();
```

## Configuration Options Reference

### 1. Basic Configuration

#### endpoints (Endpoint List)
- **Type**: `List<Endpoint>`
- **Required**: Yes
- **Description**: List of GreptimeDB server endpoints
- **Example**:
```java
// Single endpoint
.newBuilder("127.0.0.1:4001", "public")

// Multiple endpoints
.newBuilder(new String[]{"127.0.0.1:4001", "127.0.0.1:4002"}, "public")
```

#### database (Database Name)
- **Type**: `String`
- **Default**: `public`
- **Description**: Target database name. When a database name is provided, the DB will attempt to parse catalog and schema from it. We assume the format is `[<catalog>-]<schema>`:
   - If the `[<catalog>-]` part is not provided, we will use the entire database name as the schema name
   - If `[<catalog>-]` is provided, we will split the database name using `-` and use `<catalog>` and `<schema>`
- **Example**:
```java
.newBuilder("127.0.0.1:4001", "mydatabase")
```

### 2. Asynchronous Processing Configuration

#### asyncPool (Asynchronous Thread Pool)
- **Type**: `Executor`
- **Default**: `SerializingExecutor` - This executor does not start any additional threads, it only uses the current thread to batch process small tasks
- **Description**: Asynchronous thread pool used to handle various asynchronous tasks in the SDK (you are using a purely asynchronous SDK). It's generally recommended to use the default configuration. If the default implementation doesn't meet your needs, reconfigure it.
    Note: The SDK will not actively close it to release resources (if it needs to be closed) because the SDK treats it as a shared resource
- **Example**:
```java
.asyncPool(Executors.newFixedThreadPool(10))
```

### 3. RPC Configuration

The following configurations only apply to Regular API, not to Bulk API

#### rpcOptions (RPC Options)
- **Type**: `RpcOptions`
- **Default**: `RpcOptions.newDefault()`
- **Description**: RPC connection related configuration
- **Key Parameters**:
  - `useRpcSharedPool`: Whether to use global RPC shared pool, default false. By default, only gRPC internal IO threads are used to handle all tasks. It's recommended to start with the default value for your application
  - `defaultRpcTimeout`: RPC request timeout, default 60000ms (60 seconds)
  - `maxInboundMessageSize`: Maximum inbound message size, default 256MB
  - `flowControlWindow`: Flow control window size, default 256MB
  - `idleTimeoutSeconds`: Idle timeout duration, default 5 minutes
  - `keepAliveTimeSeconds`: Time without read activity before sending keep-alive ping, default Long.MAX_VALUE (disabled)
  - `keepAliveTimeoutSeconds`: Time waiting for read activity after keep-alive ping, default 3 seconds
  - `keepAliveWithoutCalls`: Whether to perform keep-alive when no outstanding RPC, default false
  - `limitKind`: gRPC layer concurrency limit algorithm, default None. Not recommended to enable without special requirements, suggest using SDK upper-layer flow control mechanisms (continue reading this document, mentioned below)
    - `None`: No concurrency limiting (default)
    - `Vegas`: TCP Vegas-based concurrency limiting algorithm
    - `Gradient`: Gradient-based concurrency limiting algorithm
  - **The following parameters only take effect when `limitKind` is set to `Vegas` or `Gradient`:**
    - `initialLimit`: Initial concurrency limit, default 64
    - `maxLimit`: Maximum concurrency limit, default 1024
    - `longRttWindow`: Long RTT window for gradient limiter, default 100 (only for Gradient algorithm)
    - `smoothing`: Smoothing factor for limit adjustment, default 0.2
    - `blockOnLimit`: Block on limit instead of failing fast, default false
    - `logOnLimitChange`: Log when limit changes, default true
  - `enableMetricInterceptor`: Enable metric interceptor, default false, will collect some gRPC layer metrics

**Example**:
```java
RpcOptions rpcOpts = RpcOptions.newDefault()
    .setDefaultRpcTimeout(30000)  // 30 seconds timeout
    .setMaxInboundMessageSize(128 * 1024 * 1024)  // 128MB
    .setKeepAliveTimeSeconds(30)  // Enable keep-alive every 30 seconds
    .setKeepAliveTimeoutSeconds(5)  // 5 seconds keep-alive timeout
    .setKeepAliveWithoutCalls(true)  // Keep-alive even without calls
    .setLimitKind(RpcOptions.LimitKind.Vegas)  // Use Vegas flow limiter
    .setInitialLimit(32)  // Start with 32 concurrent requests
    .setMaxLimit(512);  // Max 512 concurrent requests
    
.rpcOptions(rpcOpts)
```

### 4. TLS Security Configuration

#### tlsOptions (TLS Options)
- **Type**: `TlsOptions`
- **Default**: null (uses plaintext connection)
- **Description**: Enable secure connection between client and server
- **Key Parameters**:
  - `clientCertChain`: Client certificate chain file
  - `privateKey`: Private key file
  - `privateKeyPassword`: Private key password
  - `rootCerts`: Root certificate file

**Example**:
```java
TlsOptions tlsOpts = new TlsOptions();
tlsOpts.setClientCertChain(new File("client.crt"));
tlsOpts.setPrivateKey(new File("client.key")); 
tlsOpts.setPrivateKeyPassword("your-key-password");
tlsOpts.setRootCerts(new File("ca.crt"));
    
.tlsOptions(tlsOpts)
```

### 5. Write Configuration

The following configurations only apply to Regular API, not to Bulk API

#### writeMaxRetries (Write Maximum Retries)
- **Type**: `int`
- **Default**: `1`
- **Description**: Maximum number of retries for write failures. Whether to retry depends on the error type [`Status.isShouldRetry()`](https://javadoc.io/doc/io.greptime/ingester-protocol/latest/index.html)
- **Example**:
```java
.writeMaxRetries(3)
```

#### maxInFlightWritePoints (Maximum In-Flight Write Points)
- **Type**: `int`
- **Default**: `655360` (10 * 65536)
- **Description**: Write flow limit - maximum number of data points in-flight
- **Example**:
```java
.maxInFlightWritePoints(1000000)
```

#### writeLimitedPolicy (Write Limited Policy)
- **Type**: `LimitedPolicy`
- **Default**: `AbortOnBlockingTimeoutPolicy(3 seconds)`
- **Description**: Policy to use when write flow limit is exceeded
- **Available Policies**:
  - `DiscardPolicy`: Discard data if limiter is full
  - `AbortPolicy`: Abort if limiter is full, throw exception
  - `BlockingPolicy`: Block the write thread if limiter is full
  - `BlockingTimeoutPolicy`: Block for specified time then proceed if limiter is full
  - `AbortOnBlockingTimeoutPolicy`: Block for specified time, abort and throw exception if timeout

**Example**:
```java
// Use discard policy
.writeLimitedPolicy(new LimitedPolicy.DiscardPolicy())

// Use blocking timeout policy
.writeLimitedPolicy(new LimitedPolicy.BlockingTimeoutPolicy(5, TimeUnit.SECONDS))
```

#### defaultStreamMaxWritePointsPerSecond (Default Stream Write Rate)
- **Type**: `int`
- **Default**: `655360` (10 * 65536)
- **Description**: Default rate limit value (points per second) for StreamWriter
- **Example**:
```java
.defaultStreamMaxWritePointsPerSecond(100000)
```

### 6. Bulk Write Configuration

This configuration is effective for Bulk API

#### useZeroCopyWriteInBulkWrite (Zero Copy Bulk Write)
- **Type**: `boolean`
- **Default**: `true`
- **Description**: Whether to use zero-copy optimization in bulk write operations
- **Example**:
```java
.useZeroCopyWriteInBulkWrite(false)
```

### 7. Route Table Configuration

#### routeTableRefreshPeriodSeconds (Route Table Refresh Period)
- **Type**: `long`
- **Default**: `600` (10 minutes)
- **Description**: Background refresh period for route tables (seconds). Route tables will not be refreshed if value is <= 0
- **Example**:
```java
.routeTableRefreshPeriodSeconds(300)  // Refresh every 5 minutes
```

#### checkHealthTimeoutMs (Health Check Timeout)
- **Type**: `long`
- **Default**: `1000` (1 second)
- **Description**: Timeout for health check operations (milliseconds)
- **Example**:
```java
.checkHealthTimeoutMs(5000)  // 5 seconds timeout
```

### 8. Authentication Configuration

#### authInfo (Authentication Information)
- **Type**: `AuthInfo`
- **Default**: null
- **Description**: Database authentication information. Can be ignored if database doesn't require authentication
- **Example**:
```java
AuthInfo auth = new AuthInfo("username", "password");
.authInfo(auth)
```

### 9. Custom Router

#### router (Request Router)
- **Type**: `Router<Void, Endpoint>`
- **Default**: Internal default implementation
- **Description**: Custom request router. No need to set unless you have special requirements
- **Example**:
```java
.router(customRouter)
```

