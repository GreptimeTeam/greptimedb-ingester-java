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
 *
 * @author Ning Sun<sunning@greptime.com>
 */
public class TlsOptions implements Copiable<TlsOptions> {

    private Optional<File> clientCertChain = Optional.empty();

    private Optional<File> privateKey = Optional.empty();

    private Optional<String> privateKeyPassword = Optional.empty();

    private Optional<File> rootCerts = Optional.empty();

    @Override
    public TlsOptions copy() {
        TlsOptions that = new TlsOptions();

        that.setClientCertChain(this.getClientCertChain());
        that.setPrivateKey(this.getPrivateKey());
        that.setPrivateKeyPassword(this.getPrivateKeyPassword());
        that.setRootCerts(this.getRootCerts());

        return that;
    }

    public Optional<File> getClientCertChain() {
        return clientCertChain;
    }

    public void setClientCertChain(Optional<File> clientCertChain) {
        this.clientCertChain = clientCertChain;
    }

    public Optional<File> getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(Optional<File> privateKey) {
        this.privateKey = privateKey;
    }

    public Optional<String> getPrivateKeyPassword() {
        return privateKeyPassword;
    }

    public void setPrivateKeyPassword(Optional<String> privateKeyPassword) {
        this.privateKeyPassword = privateKeyPassword;
    }

    public Optional<File> getRootCerts() {
        return rootCerts;
    }

    public void setRootCerts(Optional<File> rootCerts) {
        this.rootCerts = rootCerts;
    }

    @Override
    public String toString() {
        return "TlsOptions{"
                + //
                "clientCertChain="
                + this.clientCertChain
                + //
                ", privateKey="
                + this.privateKey
                + //
                ", privateKeyPassword="
                + this.privateKeyPassword.map((v) -> "****")
                + //
                ", rootCerts="
                + this.rootCerts
                + '}';
    }
}
