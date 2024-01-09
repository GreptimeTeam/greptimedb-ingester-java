# Global Options (System properties / Java -Dxxx)

| Name                               | Description                                                                                                                                                        |
|:-----------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| greptimedb.use_os_signal           | Whether or not to use OS Signal, SDK listens for SIGUSR2 signals by default and can outputs some information. This is helpful when troubleshooting complex issues. |
| greptimedb.signal.out_dir          | Signal handler can output to the specified directory, default is the process start directory.                                                                      |
| greptimedb.available_cpus          | Specify the number of available cpus, the default is to use the full number of cpus of the current environment.                                                    |
| greptimedb.reporter.period_minutes | Metrics reporter timed output period, default 30 minutes.                                                                                                          |
| greptimedb.read.write.rw_logging   | Whether to print logs for each read/write operation, default off.                                                                                                  |