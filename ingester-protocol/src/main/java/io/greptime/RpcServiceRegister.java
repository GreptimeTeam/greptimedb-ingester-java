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

import io.greptime.rpc.MethodDescriptor;
import io.greptime.rpc.RpcFactoryProvider;
import io.greptime.v1.Database;
import io.greptime.v1.Health;

/**
 * The RPC service register.
 */
public class RpcServiceRegister {

    private static final String DATABASE_METHOD_TEMPLATE = "greptime.v1.GreptimeDatabase/%s";
    private static final String HEALTH_METHOD_TEMPLATE = "greptime.v1.HealthCheck/%s";

    public static void registerAllService() {
        // Handle
        MethodDescriptor handleMethod = MethodDescriptor.of(
                String.format(DATABASE_METHOD_TEMPLATE, "Handle"), MethodDescriptor.MethodType.UNARY, 1);
        RpcFactoryProvider.getRpcFactory()
                .register(
                        handleMethod,
                        Database.GreptimeRequest.class,
                        Database.GreptimeRequest.getDefaultInstance(),
                        Database.GreptimeResponse.getDefaultInstance());

        // HandleRequests
        MethodDescriptor handleRequestsMethod = MethodDescriptor.of(
                String.format(DATABASE_METHOD_TEMPLATE, "HandleRequests"),
                MethodDescriptor.MethodType.CLIENT_STREAMING);
        RpcFactoryProvider.getRpcFactory()
                .register(
                        handleRequestsMethod,
                        Database.GreptimeRequest.class,
                        Database.GreptimeRequest.getDefaultInstance(),
                        Database.GreptimeResponse.getDefaultInstance());

        // HealthCheck
        MethodDescriptor healthCheckMethod = MethodDescriptor.of(
                String.format(HEALTH_METHOD_TEMPLATE, "HealthCheck"), MethodDescriptor.MethodType.UNARY);
        RpcFactoryProvider.getRpcFactory()
                .register(
                        healthCheckMethod,
                        Health.HealthCheckRequest.class,
                        Health.HealthCheckRequest.getDefaultInstance(),
                        Health.HealthCheckResponse.getDefaultInstance());
    }
}
