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

package common.util;

import io.greptime.common.util.ExecutorServiceHelper;
import io.greptime.common.util.NamedThreadFactory;
import io.greptime.common.util.ThreadPoolUtil;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ExecutorServiceHelperTest {

    @Test
    public void shutdownNullTest() {
        Assert.assertTrue(ExecutorServiceHelper.shutdownAndAwaitTermination(null));
    }

    @Test
    public void shutdownNotStart() {
        ExecutorService e = ThreadPoolUtil.newBuilder()
                .poolName("test_shutdown")
                .coreThreads(1)
                .maximumThreads(1)
                .keepAliveSeconds(100L)
                .workQueue(new SynchronousQueue<>())
                .enableMetric(false)
                .threadFactory(new NamedThreadFactory("test_shutdown"))
                .build();
        Assert.assertTrue(ExecutorServiceHelper.shutdownAndAwaitTermination(e));
    }
}
