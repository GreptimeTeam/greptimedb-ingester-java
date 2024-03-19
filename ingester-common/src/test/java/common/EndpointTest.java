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

package common;

import io.greptime.common.Endpoint;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author jiachun.fjc
 */
public class EndpointTest {

    @Test
    public void testParse() {
        Endpoint ep = Endpoint.parse("192.168.1.1:18091");
        Assert.assertEquals(Endpoint.of("192.168.1.1", 18091), ep);
    }
}
