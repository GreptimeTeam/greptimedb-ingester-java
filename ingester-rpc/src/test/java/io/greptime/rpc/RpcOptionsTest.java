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

package io.greptime.rpc;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

public class RpcOptionsTest {
    @Test
    public void newDefaultShouldUseKeepAliveDefaultsTest() {
        RpcOptions options = RpcOptions.newDefault();

        Assert.assertEquals(60, options.getKeepAliveTimeSeconds());
        Assert.assertEquals(3, options.getKeepAliveTimeoutSeconds());
        Assert.assertFalse(options.isKeepAliveWithoutCalls());
    }

    @Test
    public void copyShouldHaveIndependentTlsOptions() {
        TlsOptions tlsOptions = new TlsOptions();
        tlsOptions.setRootCerts(new File("original.pem"));
        RpcOptions original = RpcOptions.newDefault();
        original.setTlsOptions(tlsOptions);

        RpcOptions copied = original.copy();

        Assert.assertNotSame(tlsOptions, copied.getTlsOptions());
        Assert.assertEquals(
                new File("original.pem"), copied.getTlsOptions().getRootCerts().get());

        copied.getTlsOptions().setRootCerts(new File("copied.pem"));
        Assert.assertEquals(
                new File("original.pem"),
                original.getTlsOptions().getRootCerts().get());
    }
}
