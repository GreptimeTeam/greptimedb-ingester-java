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

import java.util.concurrent.CompletableFuture;

/**
 * Health check. It just like to probe the database and connections.
 * Users can use this status to perform fault tolerance and disaster recovery actions.
 *
 * @author jiachun.fjc
 */
public interface HealthCheck {
    CompletableFuture<Boolean> is_alive();
}
