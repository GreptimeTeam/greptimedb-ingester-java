# Magic Tools

### How to use `kill -s SIGUSR2 $pid`

The first time you execute `kill -s SIGUSR2 $pid` you will see the following help messages on
the log output, including:

- Turn on/off the output of the condensed version of the read/write log.
- Turn on/off limiter
- Export in-memory metrics and memory state information of important objects to a local file

### Just follow the help information

```text
Handling signal SIGUSR2.
- -- GreptimeDB Signal Help --
-     Signal output dir: /Users/xxx
- 
-     How to open or close write log(The second execution means close):
-       [1] `cd /Users/xxx`
-       [2] `touch write_logging.sig`
-       [3] `kill -s SIGUSR2 $pid`
-       [4] `rm write_logging.sig`
- 
- 
-     How to open or close rpc limiter(The second execution means close):
-       [1] `cd /Users/xxx`
-       [2] `touch rpc_limit.sig`
-       [3] `kill -s SIGUSR2 $pid`
-       [4] `rm rpc_limit.sig`
- 
- 
-     How to open or close bulk write log(The second execution means close):
-       [1] `cd /Users/xxx`
-       [2] `touch bulk_write_logging.sig`
-       [3] `kill -s SIGUSR2 $pid`
-       [4] `rm bulk_write_logging.sig`
- 
-     How to get metrics and display info:
-       [1] `cd /Users/xxx`
-       [2] `rm *.sig`
-       [3] `kill -s SIGUSR2 $pid`
- 
-     The file signals that is currently open:
- 
- Displaying GreptimeDB clients triggered by signal: USR2 to file: /Users/xxx/greptimedb_client_display.log.2025-05-14_16-06-27.
- Printing GreptimeDB clients metrics triggered by signal: USR2 to file: /Users/xxx/greptimedb_client_metrics.log.2025-05-14_16-06-27.
- `BULK_WRITE_LOGGING`=true.
- `WRITE_LOGGING`=false.
- `LIMIT_SWITCH`=true.
```