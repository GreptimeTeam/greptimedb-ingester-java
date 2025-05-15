# Metrics&Display

At runtime, users can use the SIGUSR2 signal of the Linux platform to output the status information (display) of the node and the metrics.

### How

```shell
kill -s SIGUSR2 pid
```

The relevant information is output to the specified directory.

By default, 2 files are generated in the program's working directory
(cwd: `lsof -p $pid | grep cwd`)

- greptimedb_client_display.log.xxx: It records important memory state information about the
  current client
- greptimedb_client_metrics.log.xxx: It records all metrics information for the current
  client node

#### List of Metrics (constantly updated)


| Type | Name | Description |
|:-----|:-----|:------------|
| Counter | connection_counter | Total number of active connections across all endpoints |
| Counter | connection_counter_${address} | Number of active connections to the specified endpoint address |
| Counter | flight_allocation_bytes | Total bytes allocated for flight operations in bulk write API |
| Histogram | bulk_write_limiter_acquire_available_permits | Available permits for bulk write operations, indicating limiter capacity and utilization |
| Histogram | delete_rows_failure_num | Number of failed delete rows |
| Histogram | delete_rows_success_num | Number of successful delete rows |
| Histogram | insert_rows_failure_num | Number of failed insert rows |
| Histogram | insert_rows_success_num | Number of successful insert rows |
| Histogram | serializing_executor_drain_num\_${name} | Number of tasks drained by the serializing executor |
| Histogram | write_limiter_acquire_available_permits | Available permits for write operations, indicating limiter capacity and utilization |
| Meter | connection_failure | Rate of connection failures across all endpoints |
| Meter | write_by_retries_${n} | Write QPS by retry count (n=0 for first attempt, n>3 counted as n=3) |
| Meter | write_failure_num | Rate of failed write operations across all endpoints |
| Meter | write_qps | Write requests per second across all endpoints |
| Timer | async_bulk_write_pool | Execution duration of bulk write tasks in async thread pool |
| Timer | async_write_pool | Execution duration of write tasks in async thread pool |
| Timer | bulk_flight_client.wait_until_stream_ready | Time waiting for bulk write stream readiness |
| Timer | bulk_write_limiter_acquire_wait_time | Time spent waiting for bulk write permits |
| Timer | bulk_write_prepare_time | Time spent encoding data for bulk write operations |
| Timer | bulk_write_put_time | Total duration of bulk write operations from start to completion |
| Timer | direct_executor_timer_rpc_direct_pool | Execution time of RPC callbacks in current thread (default). Monitor performance and consider thread pool if needed |
| Timer | req_rt_${service_name}/${method_name} | Round-trip time of gRPC requests by service and method |
| Timer | scheduled_thread_pool.${schedule_thread_pool_name} | Task execution time in scheduled thread pool by pool name |
| Timer | serializing_executor_drain_timer_${name} | Total time to process and execute all queued tasks |
| Timer | serializing_executor_single_task_timer_${name} | Execution time per task, helping identify task-level bottlenecks |
| Timer | write_limiter_acquire_wait_time | Time waiting for write permits (excludes actual write operation time) |
| Timer | write_stream_limiter_acquire_wait_time | Time waiting for write permits when using StreamWriter (excludes actual write operation time) |

#### Example

##### greptimedb_client_display.log.xxx

```
--- GreptimeDB Client ---
id=1
version=0.14.3
endpoints=[127.0.0.1:4001]
database=public
rpcOptions=RpcOptions{useRpcSharedPool=false, defaultRpcTimeout=10000, maxInboundMessageSize=268435456, flowControlWindow=268435456, idleTimeoutSeconds=300, keepAliveTimeSeconds=9223372036854775807, keepAliveTimeoutSeconds=3, keepAliveWithoutCalls=false, limitKind=None, initialLimit=64, maxLimit=1024, longRttWindow=100, smoothing=0.2, blockOnLimit=false, logOnLimitChange=true, enableMetricInterceptor=false, tlsOptions=null}

--- RouterClient ---
opts=RouterOptions{rpcClient=io.greptime.rpc.GrpcClient@55b699ef, endpoints=[127.0.0.1:4001], refreshPeriodSeconds=600, checkHealthTimeoutMs=1000, router=null}

--- GrpcClient ---
started=true
opts=RpcOptions{useRpcSharedPool=false, defaultRpcTimeout=10000, maxInboundMessageSize=268435456, flowControlWindow=268435456, idleTimeoutSeconds=300, keepAliveTimeSeconds=9223372036854775807, keepAliveTimeoutSeconds=3, keepAliveWithoutCalls=false, limitKind=None, initialLimit=64, maxLimit=1024, longRttWindow=100, smoothing=0.2, blockOnLimit=false, logOnLimitChange=true, enableMetricInterceptor=false, tlsOptions=null}
connectionObservers=[io.greptime.GreptimeDB$RpcConnectionObserver@625d44db]
asyncPool=DirectExecutor{name='rpc_direct_pool'}
interceptors=[io.greptime.rpc.interceptors.ContextToHeadersInterceptor@275fd6f4]
managedChannelPool={127.0.0.1:4001=IdChannel{channelId=1, channel=ManagedChannelOrphanWrapper{delegate=ManagedChannelImpl{logId=5, target=127.0.0.1:4001}}}}
transientFailures={}


--- WriteClient ---
maxRetries=0
asyncPool=MetricExecutor{pool=SerializingExecutor{name='bench_async_pool'}, name='async_write_pool.time'}

--- BulkWriteClient ---
asyncPool=MetricExecutor{pool=SerializingExecutor{name='bench_async_pool'}, name='async_bulk_write_pool.time'}

```

##### greptimedb_client_metrics.log.xxx

```
-- GreptimeDB 5/14/25 4:06:27 PM =============================================================

-- GreptimeDB -- Counters --------------------------------------------------------------------
connection_counter
             count = 2
connection_counter_127.0.0.1:4001
             count = 2
flight_allocation_bytes
             count = 339503994

-- GreptimeDB -- Histograms ------------------------------------------------------------------
bulk_write_limiter_acquire_available_permits
             count = 153
               min = 0
               max = 7
              mean = 2.77
            stddev = 2.06
            median = 3.00
              75% <= 5.00
              95% <= 6.00
              98% <= 6.00
              99% <= 6.00
            99.9% <= 6.00
bulk_write_put_bytes
             count = 153
               min = 73032465
               max = 124278256
              mean = 119973603.01
            stddev = 14129538.75
            median = 124220600.00
              75% <= 124233684.00
              95% <= 124278256.00
              98% <= 124278256.00
              99% <= 124278256.00
            99.9% <= 124278256.00
bulk_write_put_rows
             count = 153
               min = 38528
               max = 65536
              mean = 63292.26
            stddev = 7454.17
            median = 65536.00
              75% <= 65536.00
              95% <= 65536.00
              98% <= 65536.00
              99% <= 65536.00
            99.9% <= 65536.00
serializing_executor_drain_num_bench_async_pool
             count = 170
               min = 1
               max = 1
              mean = 1.00
            stddev = 0.00
            median = 1.00
              75% <= 1.00
              95% <= 1.00
              98% <= 1.00
              99% <= 1.00
            99.9% <= 1.00
write_limiter_acquire_available_permits
             count = 0
               min = 0
               max = 0
              mean = 0.00
            stddev = 0.00
            median = 0.00
              75% <= 0.00
              95% <= 0.00
              98% <= 0.00
              99% <= 0.00
            99.9% <= 0.00

-- GreptimeDB -- Meters ----------------------------------------------------------------------
bulk_write_in_flight_requests
             count = 860
         mean rate = 0.84 events/second
     1-minute rate = 0.72 events/second
     5-minute rate = 0.99 events/second
    15-minute rate = 2.85 events/second
connection_failure
             count = 0
         mean rate = 0.00 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second

-- GreptimeDB -- Timers ----------------------------------------------------------------------
async_bulk_write_pool
             count = 170
         mean rate = 0.17 calls/second
     1-minute rate = 0.16 calls/second
     5-minute rate = 0.19 calls/second
    15-minute rate = 0.43 calls/second
               min = 0.02 milliseconds
               max = 9.22 milliseconds
              mean = 0.63 milliseconds
            stddev = 0.43 milliseconds
            median = 0.49 milliseconds
              75% <= 0.79 milliseconds
              95% <= 1.93 milliseconds
              98% <= 1.93 milliseconds
              99% <= 2.17 milliseconds
            99.9% <= 2.36 milliseconds
async_write_pool
             count = 0
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.00 calls/second
    15-minute rate = 0.00 calls/second
               min = 0.00 milliseconds
               max = 0.00 milliseconds
              mean = 0.00 milliseconds
            stddev = 0.00 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 0.00 milliseconds
              98% <= 0.00 milliseconds
              99% <= 0.00 milliseconds
            99.9% <= 0.00 milliseconds
bulk_flight_client.wait_until_stream_ready
             count = 153
         mean rate = 0.15 calls/second
     1-minute rate = 0.14 calls/second
     5-minute rate = 0.19 calls/second
    15-minute rate = 0.61 calls/second
               min = 0.00 milliseconds
               max = 42050.52 milliseconds
              mean = 5356.27 milliseconds
            stddev = 13869.85 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 41310.81 milliseconds
              98% <= 41310.81 milliseconds
              99% <= 41310.81 milliseconds
            99.9% <= 41734.41 milliseconds
bulk_write_limiter_acquire_wait_time
             count = 153
         mean rate = 0.15 calls/second
     1-minute rate = 0.13 calls/second
     5-minute rate = 0.18 calls/second
    15-minute rate = 0.55 calls/second
               min = 0.00 milliseconds
               max = 6742.00 milliseconds
              mean = 671.88 milliseconds
            stddev = 1774.12 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 6295.00 milliseconds
              98% <= 6410.00 milliseconds
              99% <= 6410.00 milliseconds
            99.9% <= 6410.00 milliseconds
bulk_write_prepare_time
             count = 153
         mean rate = 0.15 calls/second
     1-minute rate = 0.14 calls/second
     5-minute rate = 0.19 calls/second
    15-minute rate = 0.61 calls/second
               min = 116.00 milliseconds
               max = 42254.00 milliseconds
              mean = 5548.32 milliseconds
            stddev = 13857.03 milliseconds
            median = 192.00 milliseconds
              75% <= 278.00 milliseconds
              95% <= 41514.00 milliseconds
              98% <= 41514.00 milliseconds
              99% <= 41514.00 milliseconds
            99.9% <= 41943.00 milliseconds
bulk_write_put_time
             count = 149
         mean rate = 0.14 calls/second
     1-minute rate = 0.14 calls/second
     5-minute rate = 0.14 calls/second
    15-minute rate = 0.10 calls/second
               min = 7266.00 milliseconds
               max = 55630.00 milliseconds
              mean = 33684.43 milliseconds
            stddev = 14395.13 milliseconds
            median = 32508.00 milliseconds
              75% <= 45185.00 milliseconds
              95% <= 55221.00 milliseconds
              98% <= 55221.00 milliseconds
              99% <= 55221.00 milliseconds
            99.9% <= 55310.00 milliseconds
direct_executor_timer_rpc_direct_pool
             count = 15
         mean rate = 0.01 calls/second
     1-minute rate = 0.01 calls/second
     5-minute rate = 0.02 calls/second
    15-minute rate = 0.01 calls/second
               min = 0.00 milliseconds
               max = 0.93 milliseconds
              mean = 0.15 milliseconds
            stddev = 0.12 milliseconds
            median = 0.17 milliseconds
              75% <= 0.23 milliseconds
              95% <= 0.33 milliseconds
              98% <= 0.33 milliseconds
              99% <= 0.33 milliseconds
            99.9% <= 0.33 milliseconds
req_rt_greptime.v1.HealthCheck/HealthCheck
             count = 2
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.02 calls/second
    15-minute rate = 0.09 calls/second
               min = 12.00 milliseconds
               max = 170.00 milliseconds
              mean = 12.02 milliseconds
            stddev = 1.76 milliseconds
            median = 12.00 milliseconds
              75% <= 12.00 milliseconds
              95% <= 12.00 milliseconds
              98% <= 12.00 milliseconds
              99% <= 12.00 milliseconds
            99.9% <= 12.00 milliseconds
req_rt_greptime.v1.HealthCheck/HealthCheck_127.0.0.1:4001
             count = 2
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.02 calls/second
    15-minute rate = 0.09 calls/second
               min = 12.00 milliseconds
               max = 170.00 milliseconds
              mean = 12.02 milliseconds
            stddev = 1.76 milliseconds
            median = 12.00 milliseconds
              75% <= 12.00 milliseconds
              95% <= 12.00 milliseconds
              98% <= 12.00 milliseconds
              99% <= 12.00 milliseconds
            99.9% <= 12.00 milliseconds
scheduled_thread_pool.display_self
             count = 2
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.01 calls/second
    15-minute rate = 0.07 calls/second
               min = 1.00 milliseconds
               max = 2.00 milliseconds
              mean = 1.00 milliseconds
            stddev = 0.01 milliseconds
            median = 1.00 milliseconds
              75% <= 1.00 milliseconds
              95% <= 1.00 milliseconds
              98% <= 1.00 milliseconds
              99% <= 1.00 milliseconds
            99.9% <= 1.00 milliseconds
scheduled_thread_pool.metrics.reporter
             count = 1
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.05 calls/second
    15-minute rate = 0.13 calls/second
               min = 10.00 milliseconds
               max = 10.00 milliseconds
              mean = 10.00 milliseconds
            stddev = 0.00 milliseconds
            median = 10.00 milliseconds
              75% <= 10.00 milliseconds
              95% <= 10.00 milliseconds
              98% <= 10.00 milliseconds
              99% <= 10.00 milliseconds
            99.9% <= 10.00 milliseconds
scheduled_thread_pool.route_cache_refresher
             count = 2
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.02 calls/second
    15-minute rate = 0.09 calls/second
               min = 2.00 milliseconds
               max = 60.00 milliseconds
              mean = 2.01 milliseconds
            stddev = 0.64 milliseconds
            median = 2.00 milliseconds
              75% <= 2.00 milliseconds
              95% <= 2.00 milliseconds
              98% <= 2.00 milliseconds
              99% <= 2.00 milliseconds
            99.9% <= 2.00 milliseconds
serializing_executor_drain_timer_bench_async_pool
             count = 170
         mean rate = 0.17 calls/second
     1-minute rate = 0.16 calls/second
     5-minute rate = 0.19 calls/second
    15-minute rate = 0.43 calls/second
               min = 0.02 milliseconds
               max = 9.24 milliseconds
              mean = 0.65 milliseconds
            stddev = 0.44 milliseconds
            median = 0.51 milliseconds
              75% <= 0.82 milliseconds
              95% <= 1.99 milliseconds
              98% <= 1.99 milliseconds
              99% <= 2.24 milliseconds
            99.9% <= 2.42 milliseconds
serializing_executor_single_task_timer_bench_async_pool
             count = 170
         mean rate = 0.17 calls/second
     1-minute rate = 0.16 calls/second
     5-minute rate = 0.19 calls/second
    15-minute rate = 0.43 calls/second
               min = 0.00 milliseconds
               max = 9.00 milliseconds
              mean = 0.59 milliseconds
            stddev = 0.63 milliseconds
            median = 1.00 milliseconds
              75% <= 1.00 milliseconds
              95% <= 2.00 milliseconds
              98% <= 2.00 milliseconds
              99% <= 3.00 milliseconds
            99.9% <= 3.00 milliseconds
write_limiter_acquire_wait_time
             count = 0
         mean rate = 0.00 calls/second
     1-minute rate = 0.00 calls/second
     5-minute rate = 0.00 calls/second
    15-minute rate = 0.00 calls/second
               min = 0.00 milliseconds
               max = 0.00 milliseconds
              mean = 0.00 milliseconds
            stddev = 0.00 milliseconds
            median = 0.00 milliseconds
              75% <= 0.00 milliseconds
              95% <= 0.00 milliseconds
              98% <= 0.00 milliseconds
              99% <= 0.00 milliseconds
            99.9% <= 0.00 milliseconds

```
