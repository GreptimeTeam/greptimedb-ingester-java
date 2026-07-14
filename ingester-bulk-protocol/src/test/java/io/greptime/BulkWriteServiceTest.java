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
    public void testAttachAfterStreamErrorFailsImmediately() throws Exception {
        BulkWriteService.AsyncPutListener listener = new BulkWriteService.AsyncPutListener();
        listener.onError(CallStatus.UNAVAILABLE.toRuntimeException());

        BulkWriteService.IdentifiableCompletableFuture future = newFuture(1L);
        listener.attach(1L, future);

        Throwable failure = getFailure(future);
        Assert.assertTrue(failure instanceof FlightRuntimeException);
        Assert.assertEquals(
                FlightStatusCode.UNAVAILABLE,
                ((FlightRuntimeException) failure).status().code());
        Assert.assertEquals(0, listener.numInFlight());
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
                        Mockito.<CallOption[]>any()))
                .thenReturn(stream);

        BulkWriteService service = new BulkWriteService(manager, allocator, schema, descriptor, 60000L, 1);
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
