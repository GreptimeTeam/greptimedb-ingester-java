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

import io.greptime.common.TimeoutCompletableFuture;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.flight.BulkFlightClient.ClientStreamListener;
import org.apache.arrow.flight.BulkFlightClient.PutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class BulkWriteServiceTest {

    @Test
    public void testDiagnosticNameDerivedFromDescriptor() {
        Assert.assertEquals(
                "catalog/schema/metrics",
                BulkWriteService.diagnosticName(FlightDescriptor.path("catalog", "schema", "metrics")));
        Assert.assertEquals("unknown", BulkWriteService.diagnosticName(FlightDescriptor.command(new byte[] {1})));
    }

    @Test
    public void testAttachAfterStreamErrorFailsImmediately() throws Exception {
        BulkWriteService.AsyncPutListener listener = Mockito.spy(new BulkWriteService.AsyncPutListener());
        listener.onError(CallStatus.UNAVAILABLE.toRuntimeException());

        BulkWriteService.IdentifiableCompletableFuture future = newFuture(1L);
        listener.attach(1L, future);

        Throwable failure = getFailure(future);
        Assert.assertTrue(failure instanceof FlightRuntimeException);
        Assert.assertEquals(
                FlightStatusCode.UNAVAILABLE,
                ((FlightRuntimeException) failure).status().code());
        Assert.assertEquals(0, listener.numInFlight());
        Mockito.verify(listener, Mockito.times(1)).onError(Mockito.any());
    }

    @Test
    public void testAttachAfterNormalCompletionFailsImmediately() throws Exception {
        BulkWriteService.AsyncPutListener listener = new BulkWriteService.AsyncPutListener();
        listener.onCompleted();

        BulkWriteService.IdentifiableCompletableFuture future = newFuture(1L);
        listener.attach(1L, future);

        Assert.assertTrue(getFailure(future) instanceof IllegalStateException);
        Assert.assertEquals(0, listener.numInFlight());
    }

    @Test
    public void testNormalCompletionFailsPendingFuture() throws Exception {
        BulkWriteService.AsyncPutListener listener = new BulkWriteService.AsyncPutListener();
        BulkWriteService.IdentifiableCompletableFuture future = newFuture(1L);
        listener.attach(1L, future);

        listener.onCompleted();

        Assert.assertTrue(getFailure(future) instanceof IllegalStateException);
        Assert.assertEquals(0, listener.numInFlight());
        Assert.assertTrue(listener.isCompletedExceptionally());
    }

    @Test
    public void testConcurrentAttachAndStreamErrorAlwaysCompletesFuture() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < 100; i++) {
                BulkWriteService.AsyncPutListener listener = new BulkWriteService.AsyncPutListener();
                BulkWriteService.IdentifiableCompletableFuture future = newFuture(i + 1L);
                CountDownLatch start = new CountDownLatch(1);

                Future<?> attach = executor.submit(() -> {
                    await(start);
                    listener.attach(future.getId(), future);
                });
                Future<?> terminate = executor.submit(() -> {
                    await(start);
                    listener.onError(CallStatus.UNAVAILABLE.toRuntimeException());
                });

                start.countDown();
                attach.get(1, TimeUnit.SECONDS);
                terminate.get(1, TimeUnit.SECONDS);

                Assert.assertTrue("Future was left unresolved", future.isCompletedExceptionally());
                Assert.assertEquals(0, listener.numInFlight());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testStreamResultCompletesBeforePendingFutureCallbacksRun() throws Exception {
        BulkWriteService.AsyncPutListener listener = new BulkWriteService.AsyncPutListener();
        BulkWriteService.IdentifiableCompletableFuture future = newFuture(1L);
        AtomicBoolean callbackFinished = new AtomicBoolean();
        AtomicBoolean callbackObservedStreamError = new AtomicBoolean();
        future.whenComplete((r, t) -> {
            try {
                listener.getResult();
            } catch (FlightRuntimeException e) {
                callbackObservedStreamError.set(true);
            }
            callbackFinished.set(true);
        });
        listener.attach(1L, future);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> terminate = executor.submit(() -> listener.onError(CallStatus.UNAVAILABLE.toRuntimeException()));
            terminate.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        Assert.assertTrue(callbackFinished.get());
        Assert.assertTrue(callbackObservedStreamError.get());
    }

    @Test
    public void testWaitServerCompletedUsesConfiguredTimeout() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator, 10L);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Throwable> wait = executor.submit(() -> {
                try {
                    fixture.service.waitServerCompleted();
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            });

            try {
                Throwable failure = wait.get(1, TimeUnit.SECONDS);
                Assert.assertTrue(failure instanceof FlightRuntimeException);
                Assert.assertEquals(
                        FlightStatusCode.TIMED_OUT,
                        ((FlightRuntimeException) failure).status().code());
                Mockito.verify(fixture.stream, Mockito.timeout(1000))
                        .cancel(Mockito.eq("Bulk write stream aborted"), Mockito.isA(FlightRuntimeException.class));
            } finally {
                wait.cancel(true);
                executor.shutdownNow();
                fixture.service.close();
            }
        }
    }

    @Test
    public void testCompletedAfterAbortReportsOriginalFailure() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            FlightRuntimeException timeout =
                    CallStatus.TIMED_OUT.withDescription("message timed out").toRuntimeException();
            fixture.service.abort(timeout);

            FlightRuntimeException failure = null;
            try {
                fixture.service.completed();
                Assert.fail("Expected completion to report the abort failure");
            } catch (FlightRuntimeException e) {
                failure = e;
            }

            Assert.assertEquals(FlightStatusCode.TIMED_OUT, failure.status().code());
            Mockito.verify(fixture.stream, Mockito.never()).completed();
            fixture.service.close();
        }
    }

    @Test
    public void testCompletedReportsFailureFromConcurrentAbort() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            FlightRuntimeException timeout =
                    CallStatus.TIMED_OUT.withDescription("message timed out").toRuntimeException();
            Mockito.doAnswer(invocation -> {
                        fixture.metadataListener.onError(timeout);
                        throw new IllegalStateException("call was cancelled");
                    })
                    .when(fixture.stream)
                    .completed();

            FlightRuntimeException failure = null;
            try {
                fixture.service.completed();
                Assert.fail("Expected completion to report the concurrent abort failure");
            } catch (FlightRuntimeException e) {
                failure = e;
            }

            Assert.assertSame(timeout, failure);
            fixture.service.close();
        }
    }

    @Test
    public void testServerFailureCancelsStream() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            fixture.metadataListener.onError(CallStatus.UNAVAILABLE.toRuntimeException());

            FlightRuntimeException failure = null;
            try {
                fixture.service.waitServerCompleted();
                Assert.fail("Expected server failure");
            } catch (FlightRuntimeException e) {
                failure = e;
            }

            Assert.assertEquals(FlightStatusCode.UNAVAILABLE, failure.status().code());
            Mockito.verify(fixture.stream).cancel(Mockito.eq("Bulk write stream aborted"), Mockito.same(failure));
            fixture.service.close();
        }
    }

    @Test
    public void testMessageTimeoutCancelsStream() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator, 10L);
            Mockito.doAnswer(invocation -> {
                        ArrowBuf metadata = invocation.getArgument(0);
                        metadata.close();
                        return null;
                    })
                    .when(fixture.stream)
                    .putNext(Mockito.any());
            try (BulkWriteService service = fixture.service) {
                BulkWriteService.PutStage stage = service.putNext();

                Assert.assertTrue(
                        getFailure(stage.future()) instanceof TimeoutCompletableFuture.FutureDeadlineExceededException);
                Assert.assertTrue(fixture.metadataListener.isCompletedExceptionally());
                Mockito.verify(fixture.stream, Mockito.timeout(1000))
                        .cancel(Mockito.eq("Bulk write stream aborted"), Mockito.isA(FlightRuntimeException.class));
            }
        }
    }

    @Test
    public void testCloseCancelsActiveStream() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);

            fixture.service.close();

            Assert.assertTrue(fixture.metadataListener.isCompletedExceptionally());
            Mockito.verify(fixture.stream)
                    .cancel(Mockito.eq("Bulk write stream closed"), Mockito.isA(FlightRuntimeException.class));
        }
    }

    @Test
    public void testPutNextDoesNotSendAfterStreamTermination() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            try (BulkWriteService service = fixture.service) {
                fixture.metadataListener.onError(CallStatus.UNAVAILABLE.toRuntimeException());

                BulkWriteService.PutStage stage = service.putNext();

                Throwable failure = getFailure(stage.future());
                Assert.assertTrue(failure instanceof FlightRuntimeException);
                Assert.assertEquals(
                        FlightStatusCode.UNAVAILABLE,
                        ((FlightRuntimeException) failure).status().code());
                Mockito.verify(fixture.stream, Mockito.never()).putNext(Mockito.any());
                Assert.assertEquals(0, fixture.metadataListener.numInFlight());
            }
        }
    }

    @Test
    public void testPutNextCleansUpFutureWhenSendFailsSynchronously() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            RuntimeException sendFailure = new RuntimeException("send failed");
            Mockito.doThrow(sendFailure).when(fixture.stream).putNext(Mockito.any());

            try (BulkWriteService service = fixture.service) {
                try {
                    service.putNext();
                    Assert.fail("Expected putNext to fail");
                } catch (RuntimeException e) {
                    Assert.assertSame(sendFailure, e);
                }

                Assert.assertEquals(0, fixture.metadataListener.numInFlight());
                Assert.assertTrue(fixture.metadataListener.isCompletedExceptionally());
            }
            Mockito.verify(fixture.stream)
                    .cancel(Mockito.eq("Bulk write stream closed"), Mockito.isA(FlightRuntimeException.class));
        }
    }

    @Test
    public void testRootCleanupDoesNotMaskSynchronousSendFailure() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            BulkWriteManager manager = Mockito.mock(BulkWriteManager.class);
            ClientStreamListener stream = Mockito.mock(ClientStreamListener.class);
            VectorSchemaRoot root = Mockito.mock(VectorSchemaRoot.class);
            Schema schema = new Schema(Collections.emptyList());
            FlightDescriptor descriptor = FlightDescriptor.path("metrics");
            RuntimeException sendFailure = new RuntimeException("send failed");
            RuntimeException cleanupFailure = new RuntimeException("cleanup failed");

            Mockito.when(manager.createSchemaRoot(schema)).thenReturn(root);
            Mockito.when(manager.startPut(
                            Mockito.eq(descriptor),
                            Mockito.any(PutListener.class),
                            Mockito.eq(1L),
                            Mockito.eq(60000L),
                            Mockito.<CallOption[]>any()))
                    .thenReturn(stream);
            Mockito.doThrow(sendFailure).when(stream).putNext(Mockito.any());
            Mockito.doThrow(cleanupFailure).when(root).clear();

            try (BulkWriteService service = new BulkWriteService(manager, allocator, schema, descriptor, 60000L, 1)) {
                try {
                    service.putNext();
                    Assert.fail("Expected putNext to fail");
                } catch (RuntimeException e) {
                    Assert.assertSame(sendFailure, e);
                    Assert.assertArrayEquals(new Throwable[] {cleanupFailure}, e.getSuppressed());
                }
            }
        }
    }

    @Test
    public void testPutNextDoesNotDoubleCloseConsumedMetadataWhenWriterFails() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            RuntimeException sendFailure = new RuntimeException("send failed");
            Mockito.doAnswer(invocation -> {
                        ArrowBuf metadata = invocation.getArgument(0);
                        metadata.close();
                        Assert.assertEquals(0, metadata.refCnt());
                        throw sendFailure;
                    })
                    .when(fixture.stream)
                    .putNext(Mockito.any());

            try (BulkWriteService service = fixture.service) {
                try {
                    service.putNext();
                    Assert.fail("Expected putNext to fail");
                } catch (RuntimeException e) {
                    Assert.assertSame(sendFailure, e);
                }
            }
        }
    }

    @Test
    public void testTerminalFailureWinsOverLaterSynchronousWriterFailure() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            ServiceFixture fixture = newServiceFixture(allocator);
            RuntimeException writerFailure = new RuntimeException("writer failed");
            Mockito.doAnswer(invocation -> {
                        fixture.metadataListener.onError(CallStatus.UNAVAILABLE.toRuntimeException());
                        throw writerFailure;
                    })
                    .when(fixture.stream)
                    .putNext(Mockito.any());

            try (BulkWriteService service = fixture.service) {
                try {
                    service.putNext();
                    Assert.fail("Expected putNext to fail");
                } catch (FlightRuntimeException e) {
                    Assert.assertEquals(FlightStatusCode.UNAVAILABLE, e.status().code());
                    Assert.assertNotSame(writerFailure, e);
                }
            }
        }
    }

    @Test
    public void testTimedGetLogsBulkWriteRequestContextOnce() throws Exception {
        Logger logger = Mockito.mock(Logger.class);
        BulkWriteService.IdentifiableCompletableFuture future =
                new BulkWriteService.IdentifiableCompletableFuture(42L, 60000L, "metrics", 100, logger);

        TimeoutException timeout = null;
        try {
            future.get(1, TimeUnit.MILLISECONDS);
            Assert.fail("Expected timed get to fail");
        } catch (TimeoutException e) {
            timeout = e;
        }
        future.logTimeout(timeout, 60000L);

        Mockito.verify(logger)
                .warn(
                        "Bulk write timed out - table={}, request-id={}, rows={}, timeout={}ms",
                        "metrics",
                        42L,
                        100,
                        1L,
                        timeout);
    }

    private static BulkWriteService.IdentifiableCompletableFuture newFuture(long id) {
        return new BulkWriteService.IdentifiableCompletableFuture(id, TimeUnit.MINUTES.toMillis(1));
    }

    private static Throwable getFailure(java.util.concurrent.CompletableFuture<Integer> future) throws Exception {
        try {
            future.get(1, TimeUnit.SECONDS);
            Assert.fail("Expected future to fail");
            return null;
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static ServiceFixture newServiceFixture(BufferAllocator allocator) {
        return newServiceFixture(allocator, 60000L);
    }

    private static ServiceFixture newServiceFixture(BufferAllocator allocator, long timeoutMs) {
        BulkWriteManager manager = Mockito.mock(BulkWriteManager.class);
        ClientStreamListener stream = Mockito.mock(ClientStreamListener.class);
        Schema schema = new Schema(Collections.emptyList());
        FlightDescriptor descriptor = FlightDescriptor.path("metrics");
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArgumentCaptor<PutListener> metadataListener = ArgumentCaptor.forClass(PutListener.class);

        Mockito.when(manager.createSchemaRoot(schema)).thenReturn(root);
        Mockito.when(manager.startPut(
                        Mockito.eq(descriptor),
                        metadataListener.capture(),
                        Mockito.eq(1L),
                        Mockito.eq(timeoutMs),
                        Mockito.<CallOption[]>any()))
                .thenReturn(stream);

        BulkWriteService service = new BulkWriteService(manager, allocator, schema, descriptor, timeoutMs, 1);
        return new ServiceFixture(service, stream, (BulkWriteService.AsyncPutListener) metadataListener.getValue());
    }

    private static class ServiceFixture {
        private final BulkWriteService service;
        private final ClientStreamListener stream;
        private final BulkWriteService.AsyncPutListener metadataListener;

        private ServiceFixture(
                BulkWriteService service,
                ClientStreamListener stream,
                BulkWriteService.AsyncPutListener metadataListener) {
            this.service = service;
            this.stream = stream;
            this.metadataListener = metadataListener;
        }
    }
}
