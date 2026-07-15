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

import io.greptime.errors.LimitedException;
import io.greptime.limit.LimitedPolicy;
import io.greptime.limit.WriteLimiter;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class WriteLimitTest {

    @Test(expected = LimitedException.class)
    public void abortWriteLimitTest() throws ExecutionException, InterruptedException {
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(1, new LimitedPolicy.AbortPolicy());
        Collection<Table> rows = TestUtil.testTable("test1", 1);

        // consume the permits
        limiter.acquireAndDo(rows, CompletableFuture::new);

        limiter.acquireAndDo(rows, this::emptyOk).get();
    }

    @Test
    public void discardWriteLimitTest() throws ExecutionException, InterruptedException {
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(1, new LimitedPolicy.DiscardPolicy());
        Collection<Table> rows = TestUtil.testTable("test1", 1);

        // consume the permits
        limiter.acquireAndDo(rows, CompletableFuture::new);

        Result<WriteOk, Err> ret = limiter.acquireAndDo(rows, this::emptyOk).get();

        Assert.assertFalse(ret.isOk());
        Assert.assertEquals(Result.FLOW_CONTROL, ret.getErr().getCode());
    }

    @Test
    public void supplierFailureReleasesWritePermit() throws Exception {
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(1, new LimitedPolicy.AbortPolicy());
        Collection<Table> rows = TestUtil.testTable("test1", 1);
        RuntimeException failure = new RuntimeException("write failed");

        try {
            limiter.acquireAndDo(rows, () -> {
                throw failure;
            });
            Assert.fail("Expected write action to fail");
        } catch (RuntimeException e) {
            Assert.assertSame(failure, e);
        }

        Assert.assertTrue(limiter.acquireAndDo(rows, this::emptyOk).get().isOk());
    }

    @Test
    public void blockingWriteLimitTest() throws InterruptedException {
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(1, new LimitedPolicy.BlockingPolicy());
        Collection<Table> rows = TestUtil.testTable("test1", 1);

        // consume the permits
        limiter.acquireAndDo(rows, CompletableFuture::new);

        CountDownLatch acquiring = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread t = new Thread(() -> {
            acquiring.countDown();
            try {
                limiter.acquireAndDo(rows, this::emptyOk);
            } catch (Throwable err) {
                failure.set(err);
            }
        });
        t.start();

        acquiring.await();
        t.interrupt();
        t.join(TimeUnit.SECONDS.toMillis(1));

        Assert.assertFalse("Limiter thread did not stop after interruption", t.isAlive());
        Assert.assertTrue(failure.get() instanceof LimitedException);
        Assert.assertTrue(failure.get().getCause() instanceof InterruptedException);
    }

    @Test
    public void blockingTimeoutWriteLimitTest() throws ExecutionException, InterruptedException {
        int timeoutSecs = 2;
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(
                1, new LimitedPolicy.BlockingTimeoutPolicy(timeoutSecs, TimeUnit.SECONDS));
        Collection<Table> rows = TestUtil.testTable("test1", 1);

        // consume the permits
        limiter.acquireAndDo(rows, CompletableFuture::new);

        final long start = System.nanoTime();
        final Result<WriteOk, Err> ret =
                limiter.acquireAndDo(rows, this::emptyOk).get();
        Assert.assertEquals(timeoutSecs, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), 0.3);

        Assert.assertFalse(ret.isOk());
        Assert.assertEquals(Result.FLOW_CONTROL, ret.getErr().getCode());
    }

    @Test(expected = LimitedException.class)
    public void abortOnBlockingTimeoutWriteLimitTest() throws ExecutionException, InterruptedException {
        int timeoutSecs = 2;
        WriteLimiter limiter = new WriteClient.DefaultWriteLimiter(
                1, new LimitedPolicy.AbortOnBlockingTimeoutPolicy(timeoutSecs, TimeUnit.SECONDS));
        Collection<Table> rows = TestUtil.testTable("test1", 1);

        // consume the permits
        limiter.acquireAndDo(rows, CompletableFuture::new);

        long start = System.nanoTime();
        try {
            limiter.acquireAndDo(rows, this::emptyOk).get();
        } finally {
            Assert.assertEquals(timeoutSecs, TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), 0.3);
        }
    }

    private CompletableFuture<Result<WriteOk, Err>> emptyOk() {
        return Util.completedCf(Result.ok(WriteOk.emptyOk()));
    }
}
