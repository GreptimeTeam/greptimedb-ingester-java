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


| Type      | Name                                               | Description                                                                                                                                                                                                                                                                                                           |
|:----------|:---------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Counter   | connection_counter_${address}                      | Number of connections to the server.                                                                                                                                                                                                                                                                                  |
| Histogram | delete_rows_failure_num                            | Statistics on the number of data entries that failed to delete.                                                                                                                                                                                                                                                       |
| Histogram | delete_rows_success_num                            | Statistics on the number of successful deletions.                                                                                                                                                                                                                                                                     |
| Histogram | insert_rows_failure_num                            | Statistics on the number of data entries that failed to write.                                                                                                                                                                                                                                                        |
| Histogram | insert_rows_success_num                            | Statistics on the number of successful writes.                                                                                                                                                                                                                                                                        |
| Histogram | serializing_executor_drain_num\_${name}            | Serializing executor. Statistics on the number of draining tasks.                                                                                                                                                                                                                                                     |
| Histogram | write_limiter_acquire_available_permits            | Statistics on the number of available permits for write data(insert/delete).                                                                                                                                                                                                                                          |
| Meter     | connection_failure                                 | Statistics on the number of failed connections.                                                                                                                                                                                                                                                                       |
| Meter     | write_by_retries_${n}                              | QPS for the nth retry write, n == 0 for the first write (non-retry), n > 3 will be counted as n == 3                                                                                                                                                                                                                  |
| Meter     | write_failure_num                                  | Statistics on the number of failed writes.                                                                                                                                                                                                                                                                            |
| Meter     | write_qps                                          | Write Request QPS                                                                                                                                                                                                                                                                                                     |
| Timer     | write_stream_limiter_acquire_wait_time             | Statistics on the time spent acquiring write data (insert/delete) permits when using `StreamWriter`，<br/>note that it does not include the time spent writing, only the time spent acquiring the permit.                                                                                                              |
| Timer     | async_write_pool.time                              | Asynchronous pool time statistics for asynchronous write tasks in SDK, this is important and it is recommended to focus on it.                                                                                                                                                                                        |
| Timer     | direct_executor_timer_rpc_direct_pool              | he appearance of this metric means that we are using the current thread to execute the asynchronous callback of the rpc client, which is the default configuration.<br/> This is usually sufficient and very resource-saving, but it needs attention. When there are problems, replace it with a thread pool in time. |
| Timer     | req_rt_${service_name}/${method_name}              | The time consumption statistics of the request, the service name and method name are the names of the service and method of the grpc request.                                                                                                                                                                         |
| Timer     | scheduled_thread_pool.${schedule_thread_pool_name} | Schedule thread pool execution task time statistics.                                                                                                                                                                                                                                                                  |
| Timer     | serializing_executor_drain_timer_${name}           | Serializing executor. Drains all tasks for time consumption statistics                                                                                                                                                                                                                                                |
| Timer     | serializing_executor_single_task_timer_${name}     | Serializing executor. Single task execution time consumption statistics                                                                                                                                                                                                                                               |
| Timer     | write_limiter_acquire_wait_time                    | Statistics on the time spent acquiring write data (insert/delete) permits，<br/>note that it does not include the time spent writing, only the time spent acquiring the permit.                                                                                                                                        |

#### Example

##### greptimedb_client_display.log.xxx

```
--- GreptimeDB Client ---
id=1
version=0.5.1
endpoints=[127.0.0.1:4001]
database=public
rpcOptions=RpcOptions{useRpcSharedPool=false, defaultRpcTimeout=10000, maxInboundMessageSize=268435456, flowControlWindow=268435456, idleTimeoutSeconds=300, keepAliveTimeSeconds=9223372036854775807, keepAliveTimeoutSeconds=3, keepAliveWithoutCalls=false, limitKind=None, initialLimit=64, maxLimit=1024, longRttWindow=100, smoothing=0.2, blockOnLimit=false, logOnLimitChange=true, enableMetricInterceptor=false}

--- RouterClient ---
opts=RouterOptions{endpoints=[127.0.0.1:4001], refreshPeriodSeconds=-1, router=null}

--- GrpcClient ---
started=true
opts=RpcOptions{useRpcSharedPool=false, defaultRpcTimeout=10000, maxInboundMessageSize=268435456, flowControlWindow=268435456, idleTimeoutSeconds=300, keepAliveTimeSeconds=9223372036854775807, keepAliveTimeoutSeconds=3, keepAliveWithoutCalls=false, limitKind=None, initialLimit=64, maxLimit=1024, longRttWindow=100, smoothing=0.2, blockOnLimit=false, logOnLimitChange=true, enableMetricInterceptor=false}
connectionObservers=[io.greptime.GreptimeDB$RpcConnectionObserver@5253e7a0]
asyncPool=DirectExecutor{name='rpc-direct-pool'}
interceptors=[io.greptime.rpc.interceptors.ContextToHeadersInterceptor@1751638e]
managedChannelPool={127.0.0.1:4001=IdChannel{channelId=1, channel=ManagedChannelOrphanWrapper{delegate=ManagedChannelImpl{logId=1, target=127.0.0.1:4001}}}}
transientFailures={}


--- WriteClient ---
maxRetries=1
asyncPool=MetricExecutor{pool=SerializingExecutor{name='async_pool'}, name='async_write_pool.time'}

```

##### greptimedb_client_metrics.log.xxx

```
-- GreptimeDB 1/9/24 4:28:38 PM ==============================================================

-- GreptimeDB -- Counters --------------------------------------------------------------------
connection_counter
             count = 1
connection_counter_127.0.0.1:4001
             count = 1

-- GreptimeDB -- Histograms ------------------------------------------------------------------
delete_rows_failure_num
             count = 1
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
delete_rows_success_num
             count = 1
               min = 10
               max = 10
              mean = 10.00
            stddev = 0.00
            median = 10.00
              75% <= 10.00
              95% <= 10.00
              98% <= 10.00
              99% <= 10.00
            99.9% <= 10.00
insert_rows_failure_num
             count = 1
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
insert_rows_success_num
             count = 1
               min = 20
               max = 20
              mean = 20.00
            stddev = 0.00
            median = 20.00
              75% <= 20.00
              95% <= 20.00
              98% <= 20.00
              99% <= 20.00
            99.9% <= 20.00
serializing_executor_drain_num_async_pool
             count = 4
               min = 1
               max = 3
              mean = 2.00
            stddev = 1.00
            median = 3.00
              75% <= 3.00
              95% <= 3.00
              98% <= 3.00
              99% <= 3.00
            99.9% <= 3.00
write_limiter_acquire_available_permits
             count = 2
               min = 65516
               max = 65526
              mean = 65521.00
            stddev = 5.00
            median = 65526.00
              75% <= 65526.00
              95% <= 65526.00
              98% <= 65526.00
              99% <= 65526.00
            99.9% <= 65526.00
write_stream_limiter_acquire_wait_time
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
connection_failure
             count = 0
         mean rate = 0.00 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second
write_by_retries_0
             count = 2
         mean rate = 0.16 events/second
     1-minute rate = 0.37 events/second
     5-minute rate = 0.39 events/second
    15-minute rate = 0.40 events/second
write_failure_num
             count = 0
         mean rate = 0.00 events/second
     1-minute rate = 0.00 events/second
     5-minute rate = 0.00 events/second
    15-minute rate = 0.00 events/second
write_qps
             count = 2
         mean rate = 0.16 events/second
     1-minute rate = 0.37 events/second
     5-minute rate = 0.39 events/second
    15-minute rate = 0.40 events/second

-- GreptimeDB -- Timers ----------------------------------------------------------------------
async_write_pool.time
             count = 8
         mean rate = 0.64 calls/second
     1-minute rate = 1.47 calls/second
     5-minute rate = 1.57 calls/second
    15-minute rate = 1.59 calls/second
               min = 0.03 milliseconds
               max = 295.46 milliseconds
              mean = 37.87 milliseconds
            stddev = 97.37 milliseconds
            median = 0.36 milliseconds
              75% <= 5.27 milliseconds
              95% <= 295.46 milliseconds
              98% <= 295.46 milliseconds
              99% <= 295.46 milliseconds
            99.9% <= 295.46 milliseconds
direct_executor_timer_rpc-direct-pool
             count = 11
         mean rate = 0.88 calls/second
     1-minute rate = 2.02 calls/second
     5-minute rate = 2.16 calls/second
    15-minute rate = 2.19 calls/second
               min = 0.01 milliseconds
               max = 10.35 milliseconds
              mean = 2.54 milliseconds
            stddev = 3.23 milliseconds
            median = 1.00 milliseconds
              75% <= 5.47 milliseconds
              95% <= 10.35 milliseconds
              98% <= 10.35 milliseconds
              99% <= 10.35 milliseconds
            99.9% <= 10.35 milliseconds
req_rt_greptime.v1.GreptimeDatabase/Handle
             count = 2
         mean rate = 0.17 calls/second
     1-minute rate = 0.37 calls/second
     5-minute rate = 0.39 calls/second
    15-minute rate = 0.40 calls/second
               min = 10.00 milliseconds
               max = 591.00 milliseconds
              mean = 300.50 milliseconds
            stddev = 290.50 milliseconds
            median = 591.00 milliseconds
              75% <= 591.00 milliseconds
              95% <= 591.00 milliseconds
              98% <= 591.00 milliseconds
              99% <= 591.00 milliseconds
            99.9% <= 591.00 milliseconds
req_rt_greptime.v1.GreptimeDatabase/Handle_127.0.0.1:4001
             count = 2
         mean rate = 0.17 calls/second
     1-minute rate = 0.37 calls/second
     5-minute rate = 0.39 calls/second
    15-minute rate = 0.40 calls/second
               min = 10.00 milliseconds
               max = 591.00 milliseconds
              mean = 300.50 milliseconds
            stddev = 290.50 milliseconds
            median = 591.00 milliseconds
              75% <= 591.00 milliseconds
              95% <= 591.00 milliseconds
              98% <= 591.00 milliseconds
              99% <= 591.00 milliseconds
            99.9% <= 591.00 milliseconds
scheduled_thread_pool.display_self
             count = 1
         mean rate = 0.08 calls/second
     1-minute rate = 0.18 calls/second
     5-minute rate = 0.20 calls/second
    15-minute rate = 0.20 calls/second
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
serializing_executor_drain_timer_async_pool
             count = 8
         mean rate = 0.63 calls/second
     1-minute rate = 1.47 calls/second
     5-minute rate = 1.57 calls/second
    15-minute rate = 1.59 calls/second
               min = 0.00 milliseconds
               max = 295.59 milliseconds
              mean = 37.91 milliseconds
            stddev = 97.41 milliseconds
            median = 0.53 milliseconds
              75% <= 5.34 milliseconds
              95% <= 295.59 milliseconds
              98% <= 295.59 milliseconds
              99% <= 295.59 milliseconds
            99.9% <= 295.59 milliseconds
serializing_executor_single_task_timer_async_pool
             count = 8
         mean rate = 0.63 calls/second
     1-minute rate = 1.47 calls/second
     5-minute rate = 1.57 calls/second
    15-minute rate = 1.59 calls/second
               min = 0.00 milliseconds
               max = 295.00 milliseconds
              mean = 37.88 milliseconds
            stddev = 97.20 milliseconds
            median = 1.00 milliseconds
              75% <= 5.00 milliseconds
              95% <= 295.00 milliseconds
              98% <= 295.00 milliseconds
              99% <= 295.00 milliseconds
            99.9% <= 295.00 milliseconds
write_limiter_acquire_wait_time
             count = 2
         mean rate = 0.16 calls/second
     1-minute rate = 0.37 calls/second
     5-minute rate = 0.39 calls/second
    15-minute rate = 0.40 calls/second
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
