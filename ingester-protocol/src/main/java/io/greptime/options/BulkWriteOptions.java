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

package io.greptime.options;

import io.greptime.RouterClient;
import io.greptime.common.Copiable;
import io.greptime.models.AuthInfo;
import java.util.concurrent.Executor;

/**
 * BulkWrite options.
 */
public class BulkWriteOptions implements Copiable<BulkWriteOptions> {
    private String database;
    private AuthInfo authInfo;
    private RouterClient routerClient;
    private Executor asyncPool;
    private boolean useZeroCopyWrite;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public AuthInfo getAuthInfo() {
        return authInfo;
    }

    public void setAuthInfo(AuthInfo authInfo) {
        this.authInfo = authInfo;
    }

    public RouterClient getRouterClient() {
        return routerClient;
    }

    public void setRouterClient(RouterClient routerClient) {
        this.routerClient = routerClient;
    }

    public Executor getAsyncPool() {
        return asyncPool;
    }

    public void setAsyncPool(Executor asyncPool) {
        this.asyncPool = asyncPool;
    }

    public boolean isUseZeroCopyWrite() {
        return useZeroCopyWrite;
    }

    public void setUseZeroCopyWrite(boolean useZeroCopyWrite) {
        this.useZeroCopyWrite = useZeroCopyWrite;
    }

    @Override
    public BulkWriteOptions copy() {
        BulkWriteOptions opts = new BulkWriteOptions();
        opts.database = this.database;
        opts.authInfo = this.authInfo;
        opts.routerClient = this.routerClient;
        opts.asyncPool = this.asyncPool;
        opts.useZeroCopyWrite = this.useZeroCopyWrite;
        return opts;
    }

    @Override
    public String toString() {
        // Do not print auto info
        return "BulkWriteOptions{"
                + "database='" + database + '\''
                + ", routerClient=" + routerClient
                + ", asyncPool=" + asyncPool
                + ", useZeroCopyWrite=" + useZeroCopyWrite
                + '}';
    }
}
