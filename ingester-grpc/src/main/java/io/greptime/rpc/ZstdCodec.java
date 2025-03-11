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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.grpc.Codec;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * ZstdCodec is a Codec that uses Zstandard (zstd) for compression.
 *
 * @see <a href="https://github.com/luben/zstd-jni">Zstd-jni</a>
 */
public class ZstdCodec implements Codec {

    @Override
    public String getMessageEncoding() {
        return "zstd";
    }

    @Override
    public OutputStream compress(OutputStream os) throws IOException {
        return new ZstdOutputStream(os);
    }

    @Override
    public InputStream decompress(InputStream is) throws IOException {
        return new ZstdInputStream(is);
    }
}
