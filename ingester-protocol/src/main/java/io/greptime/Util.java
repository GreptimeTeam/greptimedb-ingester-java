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

package io.greptime;

import io.greptime.common.Display;
import io.greptime.common.Keys;
import io.greptime.common.util.ExecutorServiceHelper;
import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.ObjectPool;
import io.greptime.common.util.SharedScheduledPool;
import io.greptime.common.util.SystemPropertyUtil;
import io.greptime.common.util.ThreadPoolUtil;
import io.greptime.models.Err;
import io.greptime.rpc.Observer;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Util for GreptimeDB Ingester.
 */
public final class Util {
    private static final AtomicBoolean WRITE_LOGGING;
    private static final AtomicBoolean BULK_WRITE_LOGGING;
    private static final int REPORT_PERIOD_MIN;
    private static final ScheduledExecutorService DISPLAY;

    static {
        WRITE_LOGGING = new AtomicBoolean(SystemPropertyUtil.getBool(Keys.WRITE_LOGGING, false));
        BULK_WRITE_LOGGING = new AtomicBoolean(SystemPropertyUtil.getBool(Keys.BULK_WRITE_LOGGING, true));
        REPORT_PERIOD_MIN = SystemPropertyUtil.getInt(Keys.REPORT_PERIOD, 10);
        DISPLAY = ThreadPoolUtil.newScheduledBuilder()
                .poolName("display_self")
                .coreThreads(1)
                .enableMetric(true)
                .threadFactory(new NamedThreadFactory("display_self", true))
                .rejectedHandler(new ThreadPoolExecutor.DiscardOldestPolicy())
                .build();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> ExecutorServiceHelper.shutdownAndAwaitTermination(DISPLAY)));
    }

    /**
     * Whether to output concise write logs.
     *
     * @return true or false
     */
    public static boolean isWriteLogging() {
        return WRITE_LOGGING.get();
    }

    /**
     * See {@link #isWriteLogging()}
     *
     * Reset `writeLogging`, set to the opposite of the old value.
     *
     * @return old value
     */
    public static boolean resetWriteLogging() {
        return WRITE_LOGGING.getAndSet(!WRITE_LOGGING.get());
    }

    /**
     * Whether to output concise bulk write logs.
     *
     * @return true or false
     */
    public static boolean isBulkWriteLogging() {
        return BULK_WRITE_LOGGING.get();
    }

    /**
     * See {@link #isBulkWriteLogging()}
     *
     * Reset `bulkWriteLogging`, set to the opposite of the old value.
     *
     * @return old value
     */
    public static boolean resetBulkWriteLogging() {
        return BULK_WRITE_LOGGING.getAndSet(!BULK_WRITE_LOGGING.get());
    }

    /**
     * Auto report self period.
     *
     * @return period with minutes
     */
    public static int autoReportPeriodMin() {
        return REPORT_PERIOD_MIN;
    }

    /**
     * Only used to schedule to display the self of client.
     *
     * @param display display
     * @param printer to print the display info
     */
    public static void scheduleDisplaySelf(Display display, Display.Printer printer) {
        DISPLAY.scheduleWithFixedDelay(() -> display.display(printer), 0, autoReportPeriodMin(), TimeUnit.MINUTES);
    }

    /**
     * Create a shared scheduler pool with the given name.
     *
     * @param name scheduled pool's name
     * @param workers the num of workers
     * @return new scheduler poll instance
     */
    public static SharedScheduledPool getSharedScheduledPool(String name, int workers) {
        return new SharedScheduledPool(new ObjectPool.Resource<ScheduledExecutorService>() {

            @Override
            public ScheduledExecutorService create() {
                return ThreadPoolUtil.newScheduledBuilder()
                        .poolName(name)
                        .coreThreads(workers)
                        .enableMetric(true)
                        .threadFactory(new NamedThreadFactory(name, true))
                        .rejectedHandler(new ThreadPoolExecutor.DiscardOldestPolicy())
                        .build();
            }

            @Override
            public void close(ScheduledExecutorService instance) {
                ExecutorServiceHelper.shutdownAndAwaitTermination(instance);
            }
        });
    }

    /**
     * Returns a new CompletableFuture that is already completed with the given
     * value. Same as {@link CompletableFuture#completedFuture(Object)}, only
     * rename the method.
     *
     * @param value the given value
     * @param <U> the type of the value
     * @return the completed {@link CompletableFuture}
     */
    public static <U> CompletableFuture<U> completedCf(U value) {
        return CompletableFuture.completedFuture(value);
    }

    /**
     * Returns a new CompletableFuture that is already exceptionally with the given
     * error.
     *
     * @param t the given exception
     * @param <U> the type of the value
     * @return the exceptionally {@link CompletableFuture}
     */
    public static <U> CompletableFuture<U> errorCf(Throwable t) {
        CompletableFuture<U> err = new CompletableFuture<>();
        err.completeExceptionally(t);
        return err;
    }

    public static <V> Observer<V> toObserver(CompletableFuture<V> future) {
        return new Observer<V>() {

            @Override
            public void onNext(V value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable err) {
                future.completeExceptionally(err);
            }
        };
    }

    public static long randomInitialDelay(long delay) {
        return ThreadLocalRandom.current().nextLong(delay, delay << 1);
    }

    public static boolean shouldNotRetry(Err err) {
        return !shouldRetry(err);
    }

    public static boolean shouldRetry(Err err) {
        if (err == null) {
            return false;
        }
        Status status = Status.parse(err.getCode());
        return status != null && status.isShouldRetry();
    }

    /**
     * Returns the version of this client.
     *
     * @return the version of this client
     */
    public static String clientVersion() {
        try {
            return loadProps(Util.class.getClassLoader(), "client_version.properties")
                    .getProperty(Keys.VERSION_KEY, "Unknown version");
        } catch (Exception ignored) {
            return "Unknown version(err)";
        }
    }

    public static Properties loadProps(ClassLoader loader, String name) throws IOException {
        Properties prop = new Properties();
        prop.load(loader.getResourceAsStream(name));
        return prop;
    }
}
