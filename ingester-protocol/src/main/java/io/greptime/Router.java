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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * RPC router for GreptimeDB.
 *
 * @author jiachun.fjc
 */
public interface Router<R, E> {

    /**
     * For a given request return the routing decision for the call.
     *
     * @param request route request
     * @return a endpoint for the call
     */
    CompletableFuture<E> routeFor(R request);

    /**
     * Refresh the routing table from remote server.
     * @return a future that will be completed when the refresh is done
     */
    CompletableFuture<Boolean> refresh();

    /**
     * Refresh the routing table.
     * We need to get all the endpoints, and this method will overwrite all
     * current endpoints.
     *
     * @param endpoints all new endpoints
     */
    void onRefresh(List<E> endpoints);
}
