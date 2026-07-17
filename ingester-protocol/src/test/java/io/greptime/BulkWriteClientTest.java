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

import io.greptime.common.Endpoint;
import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import io.greptime.options.BulkWriteOptions;
import io.greptime.rpc.Context;
import io.greptime.rpc.RpcOptions;
import io.greptime.rpc.TlsOptions;
import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BulkWriteClientTest {

    @Test
    public void testBulkStreamWriterForwardsRpcOptionsToManager() {
        Endpoint endpoint = Endpoint.of("127.0.0.1", 4001);
        RouterClient routerClient = Mockito.mock(RouterClient.class);
        Mockito.when(routerClient.route()).thenReturn(CompletableFuture.completedFuture(endpoint));

        RpcOptions rpcOptions = new RpcOptions();
        rpcOptions.setMaxInboundMessageSize(1_234_567);
        rpcOptions.setFlowControlWindow(2_345_678);
        rpcOptions.setIdleTimeoutSeconds(41);
        rpcOptions.setKeepAliveTimeSeconds(42);
        rpcOptions.setKeepAliveTimeoutSeconds(43);
        rpcOptions.setKeepAliveWithoutCalls(true);
        TlsOptions tlsOptions = new TlsOptions();
        tlsOptions.setRootCerts(new File("test-root.crt"));
        rpcOptions.setTlsOptions(tlsOptions);

        BulkWriteOptions options = new BulkWriteOptions();
        options.setDatabase("test_db");
        options.setRouterClient(routerClient);
        options.setAsyncPool(Runnable::run);
        options.setRpcOptions(rpcOptions);

        BulkWriteManager manager = Mockito.mock(BulkWriteManager.class);
        BulkWriteService service = Mockito.mock(BulkWriteService.class);
        Mockito.when(manager.intoBulkWriteStream(
                        Mockito.eq("test_table"),
                        Mockito.any(),
                        Mockito.eq(1_000L),
                        Mockito.eq(2),
                        Mockito.any(),
                        Mockito.any()))
                .thenReturn(service);
        CapturingBulkWriteClient client = new CapturingBulkWriteClient(manager);
        client.init(options);

        TableSchema schema = TableSchema.newBuilder("test_table")
                .addField("value", DataType.Int64)
                .build();
        BulkStreamWriter writer = client.bulkStreamWriter(schema, 128, 4096, 1_000, 2, Context.newDefault());

        Assert.assertNotNull(writer);
        Assert.assertEquals(endpoint, client.endpoint);
        Assert.assertEquals(128, client.allocatorInitReservation);
        Assert.assertEquals(4096, client.allocatorMaxAllocation);
        Assert.assertEquals(ArrowCompressionType.None, client.compressionType);
        Assert.assertEquals(1_234_567, client.rpcOptions.getMaxInboundMessageSize());
        Assert.assertEquals(2_345_678, client.rpcOptions.getFlowControlWindow());
        Assert.assertEquals(41, client.rpcOptions.getIdleTimeoutSeconds());
        Assert.assertEquals(42, client.rpcOptions.getKeepAliveTimeSeconds());
        Assert.assertEquals(43, client.rpcOptions.getKeepAliveTimeoutSeconds());
        Assert.assertTrue(client.rpcOptions.isKeepAliveWithoutCalls());
        Assert.assertEquals(
                new File("test-root.crt"),
                client.rpcOptions.getTlsOptions().getRootCerts().orElse(null));
        Mockito.verify(service).start();
    }

    @Test
    public void testTimedGetReportsCallerTimeoutToPutStage() throws Exception {
        BulkWriteService.PutStage stage = Mockito.mock(BulkWriteService.PutStage.class);
        CompletableFuture<Integer> future = new BulkWriteClient.TimeoutLoggingFuture(new CompletableFuture<>(), stage);

        TimeoutException timeout = null;
        try {
            future.get(1, TimeUnit.MILLISECONDS);
            Assert.fail("Expected timed get to fail");
        } catch (TimeoutException e) {
            timeout = e;
        }

        Mockito.verify(stage).logTimeout(timeout, 1, TimeUnit.MILLISECONDS);
    }

    private static class CapturingBulkWriteClient extends BulkWriteClient {
        private final BulkWriteManager manager;
        private Endpoint endpoint;
        private long allocatorInitReservation;
        private long allocatorMaxAllocation;
        private ArrowCompressionType compressionType;
        private RpcOptions rpcOptions;

        private CapturingBulkWriteClient(BulkWriteManager manager) {
            this.manager = manager;
        }

        @Override
        BulkWriteManager createBulkWriteManager(
                Endpoint endpoint,
                long allocatorInitReservation,
                long allocatorMaxAllocation,
                ArrowCompressionType compressionType,
                RpcOptions rpcOptions) {
            this.endpoint = endpoint;
            this.allocatorInitReservation = allocatorInitReservation;
            this.allocatorMaxAllocation = allocatorMaxAllocation;
            this.compressionType = compressionType;
            this.rpcOptions = rpcOptions;
            return this.manager;
        }
    }
}
