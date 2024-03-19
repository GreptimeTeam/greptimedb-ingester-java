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

import io.greptime.common.Copiable;
import java.io.File;
import java.util.Optional;

/**
 * GreptimeDB secure connection options
 */
public class TlsOptions implements Copiable<TlsOptions> {

    private File clientCertChain;

    private File privateKey;

    private String privateKeyPassword;

    private File rootCerts;

    @Override
    public TlsOptions copy() {
        TlsOptions that = new TlsOptions();

        that.setClientCertChain(this.clientCertChain);
        that.setPrivateKey(this.privateKey);
        that.setPrivateKeyPassword(this.privateKeyPassword);
        that.setRootCerts(this.rootCerts);

        return that;
    }

    public Optional<File> getClientCertChain() {
        return Optional.ofNullable(this.clientCertChain);
    }

    public void setClientCertChain(File clientCertChain) {
        this.clientCertChain = clientCertChain;
    }

    public Optional<File> getPrivateKey() {
        return Optional.ofNullable(this.privateKey);
    }

    public void setPrivateKey(File privateKey) {
        this.privateKey = privateKey;
    }

    public Optional<String> getPrivateKeyPassword() {
        return Optional.ofNullable(this.privateKeyPassword);
    }

    public void setPrivateKeyPassword(String privateKeyPassword) {
        this.privateKeyPassword = privateKeyPassword;
    }

    public Optional<File> getRootCerts() {
        return Optional.ofNullable(this.rootCerts);
    }

    public void setRootCerts(File rootCerts) {
        this.rootCerts = rootCerts;
    }

    @Override
    public String toString() {
        return "TlsOptions{" + //
                "clientCertChain="
                + clientCertChain + //
                ", privateKey="
                + privateKey + //
                ", privateKeyPassword='"
                + getPrivateKeyPassword().map((v) -> "****") + '\'' + //
                ", rootCerts="
                + rootCerts + //
                '}';
    }
}
