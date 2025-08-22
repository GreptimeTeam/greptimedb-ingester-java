# GreptimeOptions 配置指南

## 概述

`GreptimeOptions` 是 GreptimeDB Java 客户端的主要配置类，用于配置客户端连接、写入选项、RPC 设置以及其他各种参数。本文档提供了所有配置选项的详细信息，包括其用法和默认值。

## 基本用法

```java
// 创建基本配置
GreptimeOptions options = GreptimeOptions.newBuilder("127.0.0.1:4001", "public")
    .build();

// 创建多端点配置
GreptimeOptions options = GreptimeOptions.newBuilder(
    new String[]{"127.0.0.1:4001", "127.0.0.1:4002"}, "public")
    .build();
```

## 配置选项参考

### 1. 基础配置

#### endpoints (端点列表)
- **类型**: `List<Endpoint>`
- **必需**: 是
- **描述**: GreptimeDB 服务器端点列表
- **示例**:
```java
// 单个端点
.newBuilder("127.0.0.1:4001", "public")

// 多个端点
.newBuilder(new String[]{"127.0.0.1:4001", "127.0.0.1:4002"}, "public")
```

#### database (数据库名称)
- **类型**: `String`
- **默认值**: `public`
- **描述**: 目标数据库名称，当提供数据库名称时，DB 端会尝试从中解析 catalog 和 schema。我们假设格式为 `[<catalog>-]<schema>`：
   - 如果未提供 `[<catalog>-]` 部分，我们将整个数据库名称用作 schema 名称
   - 如果提供了 `[<catalog>-]`，我们将使用 `-` 分割数据库名称，并使用 `<catalog>` 和 `<schema>`。
- **示例**:
```java
.newBuilder("127.0.0.1:4001", "mydatabase")
```

### 2. 异步处理配置

#### asyncPool (异步线程池)
- **类型**: `Executor`
- **默认值**: `SerializingExecutor` 这个 executor 不会启动任何额外线程，它只利用当前线程去批量完成小的任务
- **描述**: 异步线程池，用于处理 SDK 中的各种异步任务（你正在使用一个纯异步的 SDK）。如果你不设置它，通常建议使用默认配置，如果默认实现不满足你的需求时重新配置
    注意：SDK 内部不会主动将其关闭以释放资源（如果需要关闭），因为 SDK 会视其为共享资源
- **示例**:
```java
.asyncPool(Executors.newFixedThreadPool(10))
```

### 3. RPC 配置

以下配置只针对 Regular API，不对 Bulk API 生效

#### rpcOptions (RPC 选项)
- **类型**: `RpcOptions`
- **默认值**: `RpcOptions.newDefault()`
- **描述**: RPC 连接相关配置
- **主要参数**:
  - `useRpcSharedPool`: 是否使用全局 RPC 共享池，默认 false，即默认只使用 gRPC 内部的 IO 线程来处理所有任务，建议从默认值开始你的应用
  - `defaultRpcTimeout`: RPC 请求超时时间，默认 60000ms（60 秒）
  - `maxInboundMessageSize`: 最大入站消息大小，默认 256 MB
  - `flowControlWindow`: 流控窗口大小，默认 256 MB
  - `idleTimeoutSeconds`: 空闲超时时间，默认 5 分钟
  - `keepAliveTimeSeconds`: 发送保活 ping 前的无读取活动时间，默认 Long.MAX_VALUE（禁用）
  - `keepAliveTimeoutSeconds`: 保活 ping 后等待读取活动的时间，默认 3 秒
  - `keepAliveWithoutCalls`: 无未完成 RPC 时是否执行保活，默认 false
  - `limitKind`: gRPC 层的并发限制算法，默认 None，没有特殊需求不建议开启，建议使用 SDK 上层的限流机制（可继续阅读该文档，下面会提到）
    - `None`: 无并发限制（默认）
    - `Vegas`: 基于 TCP Vegas 的并发限制算法
    - `Gradient`: 基于梯度的并发限制算法
  - **以下参数仅在 `limitKind` 设置为 `Vegas` 或 `Gradient` 时生效：**
    - `initialLimit`: 初始并发限制，默认 64
    - `maxLimit`: 最大并发限制，默认 1024
    - `longRttWindow`: 梯度限制器的长 RTT 窗口，默认 100（仅用于 Gradient 算法）
    - `smoothing`: 限制调整的平滑因子，默认 0.2
    - `blockOnLimit`: 达到限制时阻塞而非快速失败，默认 false
    - `logOnLimitChange`: 限制变化时记录日志，默认 true
  - `enableMetricInterceptor`: 启用指标拦截器，默认 false，会采集部分 gRPC 层的指标

**示例**:
```java
RpcOptions rpcOpts = RpcOptions.newDefault()
    .setDefaultRpcTimeout(30000)  // 30 秒超时
    .setMaxInboundMessageSize(128 * 1024 * 1024)  // 128 MB
    .setKeepAliveTimeSeconds(30)  // 每 30 秒启用保活
    .setKeepAliveTimeoutSeconds(5)  // 5 秒保活超时
    .setKeepAliveWithoutCalls(true)  // 即使没有调用也保活
    .setLimitKind(RpcOptions.LimitKind.Vegas)  // 使用 Vegas 限流器
    .setInitialLimit(32)  // 从 32 个并发请求开始
    .setMaxLimit(512);  // 最大 512 个并发请求
    
.rpcOptions(rpcOpts)
```

### 4. TLS 安全配置

#### tlsOptions (TLS 选项)
- **类型**: `TlsOptions`
- **默认值**: null (使用明文连接)
- **描述**: 启用客户端和服务器之间的安全连接
- **主要参数**:
  - `clientCertChain`: 客户端证书链文件
  - `privateKey`: 私钥文件
  - `privateKeyPassword`: 私钥密码
  - `rootCerts`: 根证书文件

**示例**:
```java
TlsOptions tlsOpts = new TlsOptions();
tlsOpts.setClientCertChain(new File("client.crt"));
tlsOpts.setPrivateKey(new File("client.key")); 
tlsOpts.setPrivateKeyPassword("your-key-password");
tlsOpts.setRootCerts(new File("ca.crt"));
    
.tlsOptions(tlsOpts)
```

### 5. 写入配置

以下配置只针对 Regular API，不对 Bulk API 生效

#### writeMaxRetries (写入最大重试次数)
- **类型**: `int`
- **默认值**: `1`
- **描述**: 写入失败时的最大重试次数，是否重试取决于错误类型 [`Status.isShouldRetry()`](https://javadoc.io/doc/io.greptime/ingester-protocol/latest/index.html)
- **示例**:
```java
.writeMaxRetries(3)
```

#### maxInFlightWritePoints (最大并发写入点数)
- **类型**: `int`
- **默认值**: `655360` (10 * 65536)
- **描述**: 写入流量限制 - 最大并发数据点数量
- **示例**:
```java
.maxInFlightWritePoints(1000000)
```

#### writeLimitedPolicy (写入限流策略)
- **类型**: `LimitedPolicy`
- **默认值**: `AbortOnBlockingTimeoutPolicy（3 秒）`
- **描述**: 当写入流量限制被超过时使用的策略
- **可用策略**:
  - `DiscardPolicy`: 如果限制器已满则丢弃数据
  - `AbortPolicy`: 如果限制器已满则中止，抛出异常
  - `BlockingPolicy`: 如果限制器已满则阻塞写入线程
  - `BlockingTimeoutPolicy`: 如果限制器已满则阻塞指定的时间后再放行
  - `AbortOnBlockingTimeoutPolicy`: 阻塞指定时间，超时则中止并抛出异常

**示例**:
```java
// 使用丢弃策略
.writeLimitedPolicy(new LimitedPolicy.DiscardPolicy())

// 使用阻塞超时策略
.writeLimitedPolicy(new LimitedPolicy.BlockingTimeoutPolicy(5, TimeUnit.SECONDS))
```

#### defaultStreamMaxWritePointsPerSecond (默认流写入速率)
- **类型**: `int`
- **默认值**: `655360` (10 * 65536)
- **描述**: StreamWriter 的默认速率限制值（每秒点数）
- **示例**:
```java
.defaultStreamMaxWritePointsPerSecond(100000)
```

### 6. 批量写入配置

这个配置对 Bulk API 有效

#### useZeroCopyWriteInBulkWrite (批量写入零拷贝)
- **类型**: `boolean`
- **默认值**: `true`
- **描述**: 在批量写入操作中是否使用零拷贝优化
- **示例**:
```java
.useZeroCopyWriteInBulkWrite(false)
```

### 7. 路由表配置

#### routeTableRefreshPeriodSeconds (路由表刷新周期)
- **类型**: `long`
- **默认值**: `600`（10 分钟）
- **描述**: 后台刷新路由表的周期（秒）。如果值 <= 0，则不会刷新路由表
- **示例**:
```java
.routeTableRefreshPeriodSeconds(300)  // 每 5 分钟刷新一次
```

#### checkHealthTimeoutMs (健康检查超时)
- **类型**: `long`
- **默认值**: `1000`（1 秒）
- **描述**: 健康检查操作的超时时间（毫秒）
- **示例**:
```java
.checkHealthTimeoutMs(5000)  // 5 秒超时
```

### 8. 认证配置

#### authInfo (认证信息)
- **类型**: `AuthInfo`
- **默认值**: null
- **描述**: 数据库认证信息。如果数据库不需要认证可以忽略
- **示例**:
```java
AuthInfo auth = new AuthInfo("username", "password");
.authInfo(auth)
```

### 9. 自定义路由器

#### router (请求路由器)
- **类型**: `Router<Void, Endpoint>`
- **默认值**: 内部默认实现
- **描述**: 自定义请求路由器。除非有特殊需求，否则无需设置
- **示例**:
```java
.router(customRouter)
```
