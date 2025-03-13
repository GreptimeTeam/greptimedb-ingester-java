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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class ZstdCodecTest {

    @Test
    public void testZstdCodec() throws IOException {
        ZstdCodec codec = new ZstdCodec();
        String data = "Hello, World!";
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStream os = codec.compress(baos);
        os.write(dataBytes);
        os.close();
        try (InputStream is = codec.decompress(new ByteArrayInputStream(baos.toByteArray()))) {
            byte[] result = new byte[dataBytes.length];
            is.read(result);
            Assert.assertEquals(data, new String(result, StandardCharsets.UTF_8));
        }
    }
}
