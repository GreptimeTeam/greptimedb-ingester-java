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

import io.greptime.Router;
import io.greptime.common.Copiable;
import io.greptime.common.Endpoint;
import io.greptime.rpc.RpcClient;
import java.util.List;

/**
 * Router options.
 */
public class RouterOptions implements Copiable<RouterOptions> {

    private RpcClient rpcClient;
    private List<Endpoint> endpoints;

    // Refresh frequency of route tables. The background refreshes
    // all route tables periodically. If the value is less than or
    // equal to 0, the route tables will not be refreshed.
    private long refreshPeriodSeconds = -1;
    private Router<Void, Endpoint> router;

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public void setRpcClient(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public List<Endpoint> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }

    public long getRefreshPeriodSeconds() {
        return refreshPeriodSeconds;
    }

    public void setRefreshPeriodSeconds(long refreshPeriodSeconds) {
        this.refreshPeriodSeconds = refreshPeriodSeconds;
    }

    public Router<Void, Endpoint> getRouter() {
        return router;
    }

    public void setRouter(Router<Void, Endpoint> router) {
        this.router = router;
    }

    @Override
    public RouterOptions copy() {
        RouterOptions opts = new RouterOptions();
        opts.rpcClient = rpcClient;
        opts.endpoints = this.endpoints;
        opts.refreshPeriodSeconds = this.refreshPeriodSeconds;
        opts.router = this.router;
        return opts;
    }

    @Override
    public String toString() {
        return "RouterOptions{" + "endpoints="
                + endpoints + ", refreshPeriodSeconds="
                + refreshPeriodSeconds + ", router="
                + router + '}';
    }
}
