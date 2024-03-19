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

package io.greptime.common.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link java.util.concurrent.ThreadPoolExecutor} that can additionally
 * schedule tasks to run after a given delay with a logger witch can print
 * error message for failed execution.
 *
 * @author jiachun.fjc
 */
public class LogScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(LogScheduledThreadPoolExecutor.class);

    private final int corePoolSize;
    private final String name;

    public LogScheduledThreadPoolExecutor(
            int corePoolSize, //
            ThreadFactory threadFactory, //
            RejectedExecutionHandler handler, //
            String name) {
        super(corePoolSize, threadFactory, handler);
        this.corePoolSize = corePoolSize;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);

        if (t == null && r instanceof Future<?>) {
            try {
                Future<?> f = (Future<?>) r;
                if (f.isDone()) {
                    f.get();
                }
            } catch (CancellationException | ExecutionException ee) {
                t = ee.getCause();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt(); // ignore/reset
            }
        }
        if (t != null) {
            LOG.error("Uncaught exception in pool: {}, {}.", this.name, super.toString(), t);
        }
    }

    @Override
    protected void terminated() {
        super.terminated();

        LOG.info("ThreadPool is terminated: {}, {}.", this.name, super.toString());
    }

    @Override
    public String toString() {
        return "ScheduledThreadPoolExecutor {" + //
                "corePoolSize="
                + corePoolSize + //
                ", name='"
                + name + '\'' + //
                "} "
                + super.toString();
    }
}
