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

package org.apache.arrow.flight;

import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

// Version-coupled to grpc-netty internals; keep all private-field knowledge isolated here.
final class NettyChannelBuilderInspector {
    private final NettyChannelBuilder builder;

    private NettyChannelBuilderInspector(NettyChannelBuilder builder) {
        this.builder = builder;
    }

    static NettyChannelBuilderInspector inspect(NettyChannelBuilder builder) {
        return new NettyChannelBuilderInspector(builder);
    }

    int maxInboundMessageSize() throws Exception {
        return (Integer) field(builder, "maxInboundMessageSize");
    }

    int flowControlWindow() throws Exception {
        return (Integer) field(builder, "flowControlWindow");
    }

    long idleTimeoutSeconds() throws Exception {
        long timeoutMillis = (Long) field(delegate(), "idleTimeoutMillis");
        return TimeUnit.MILLISECONDS.toSeconds(timeoutMillis);
    }

    long keepAliveTimeSeconds() throws Exception {
        long timeNanos = (Long) field(builder, "keepAliveTimeNanos");
        return TimeUnit.NANOSECONDS.toSeconds(timeNanos);
    }

    long keepAliveTimeoutSeconds() throws Exception {
        long timeoutNanos = (Long) field(builder, "keepAliveTimeoutNanos");
        return TimeUnit.NANOSECONDS.toSeconds(timeoutNanos);
    }

    boolean keepAliveWithoutCalls() throws Exception {
        return (Boolean) field(builder, "keepAliveWithoutCalls");
    }

    int maxTraceEvents() throws Exception {
        return (Integer) field(delegate(), "maxTraceEvents");
    }

    NegotiationType negotiationType() throws Exception {
        Object negotiator = field(builder, "protocolNegotiatorFactory");
        return (NegotiationType) field(negotiator, "negotiationType");
    }

    private Object delegate() throws Exception {
        return field(builder, "managedChannelImplBuilder");
    }

    private static Object field(Object target, String name) throws Exception {
        Class<?> type = target.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }
}
