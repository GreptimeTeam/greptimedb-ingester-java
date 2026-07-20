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
import io.greptime.rpc.RpcOptions;
import java.util.ArrayList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class BulkWriteManagerTest {

    @Test
    public void testCreateFailureClosesChildAllocator() throws Exception {
        try (BufferAllocator parentAllocator = new RootAllocator(Long.MAX_VALUE)) {
            RuntimeException constructionFailure = new RuntimeException("channel configuration failed");
            RpcOptions rpcOptions = new RpcOptions() {
                @Override
                public RpcOptions copy() {
                    return this;
                }

                @Override
                public int getFlowControlWindow() {
                    throw constructionFailure;
                }
            };
            BulkWriteManager manager = null;
            RuntimeException failure = null;
            int childrenAfterFailure;
            try {
                manager = BulkWriteManager.createWithRpcOptions(
                        parentAllocator,
                        Endpoint.of("localhost", 4001),
                        0,
                        Long.MAX_VALUE,
                        ArrowCompressionType.None,
                        rpcOptions);
                Assert.fail("Expected invalid flow control window to fail");
            } catch (RuntimeException e) {
                failure = e;
            } finally {
                childrenAfterFailure = parentAllocator.getChildAllocators().size();
                try {
                    if (manager != null) {
                        manager.close();
                    }
                } finally {
                    for (BufferAllocator child : new ArrayList<>(parentAllocator.getChildAllocators())) {
                        child.close();
                    }
                }
            }

            Assert.assertSame(constructionFailure, failure);
            Assert.assertEquals(0, childrenAfterFailure);
        }
    }
}
