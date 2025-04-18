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

package io.greptime.common;

import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.ThreadPoolUtil;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link CompletableFuture} that will complete exceptionally if the operation takes too long.
 */
public class TimeoutCompletableFuture<T> extends CompletableFuture<T> {

    private static final ScheduledExecutorService SCHEDULER = ThreadPoolUtil.newScheduledBuilder()
            .enableMetric(false)
            .coreThreads(1)
            .poolName("timeout-future-scheduler")
            .threadFactory(new NamedThreadFactory("timeout-future-scheduler", true))
            .build();

    private final long timeout;
    private final TimeUnit unit;

    public TimeoutCompletableFuture(long timeout, TimeUnit unit) {
        this.timeout = timeout;
        this.unit = unit;
    }

    /**
     * Schedule the timeout for the future.
     *
     * @return this future
     */
    public TimeoutCompletableFuture<T> scheduleTimeout() {
        SCHEDULER.schedule(
                () -> {
                    if (isCancelled() || isDone()) {
                        return;
                    }
                    completeExceptionally(new FutureDeadlineExceededException(
                            "Future deadline exceeded, timeout: " + this.timeout + " " + this.unit));
                },
                this.timeout,
                this.unit);

        return this;
    }

    public static class FutureDeadlineExceededException extends TimeoutException {
        private static final long serialVersionUID = 1L;

        public FutureDeadlineExceededException(String message) {
            super(message);
        }
    }
}
