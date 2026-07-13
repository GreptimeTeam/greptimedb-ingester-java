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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class BulkWriteServiceTest {

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
}
