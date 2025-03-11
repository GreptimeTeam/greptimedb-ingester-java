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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Invoke context, it can pass some additional information to the
 * database server in the form of KV.
 */
@SuppressWarnings({"unchecked"})
public class Context implements Copiable<Context> {

    private static final String HINTS_KEY = "x-greptime-hints";

    private final Map<String, Object> ctx = new HashMap<>();

    private Compression compression = Compression.None;

    /**
     * Creates a new {@link Context} with empty values.
     */
    public static Context newDefault() {
        return new Context();
    }

    /**
     * Creates a new {@link Context} with the specified key-value pair.
     *
     * @param key the key
     * @param value the value
     * @return the new {@link Context}
     */
    public static Context of(String key, Object value) {
        return new Context().with(key, value);
    }

    /**
     * Sets the compression for the context.
     *
     * @param compression the compression type
     * @return this {@link Context}
     */
    public Context withCompression(Compression compression) {
        synchronized (this) {
            this.compression = compression;
        }
        return this;
    }

    /**
     * Gets the compression for the context.
     *
     * @return the compression type
     */
    public Compression getCompression() {
        return this.compression;
    }

    /**
     * Adds the specified key-value pair to this {@link Context}.
     *
     * @param key the key
     * @param value the value
     * @return this {@link Context}
     */
    public Context with(String key, Object value) {
        synchronized (this) {
            this.ctx.put(key, value);
        }
        return this;
    }

    /**
     * Adds a hint to the context.
     *
     * @param key the key
     * @param value the value
     * @return this {@link Context}
     */
    public Context withHint(String key, String value) {
        synchronized (this) {
            this.ctx.compute(HINTS_KEY, (k, v) -> {
                if (v == null) {
                    v = String.format("%s=%s", key, value);
                } else {
                    v = v + "," + String.format("%s=%s", key, value);
                }
                return v;
            });
        }
        return this;
    }

    /**
     * Gets the hints from the context.
     *
     * @return the hints
     */
    public String getHints() {
        synchronized (this) {
            return (String) this.ctx.get(HINTS_KEY);
        }
    }

    /**
     * Gets the value of the specified key.
     *
     * @param key the key
     * @return the value
     * @param <T> the type of the value
     */
    public <T> T get(String key) {
        synchronized (this) {
            return (T) this.ctx.get(key);
        }
    }

    /**
     * Removes the specified key and its value form this {@link Context}.
     *
     * @param key the key
     * @return the removed value
     * @param <T> the type of the value
     */
    public <T> T remove(String key) {
        synchronized (this) {
            return (T) this.ctx.remove(key);
        }
    }

    /**
     * Gets the value of the specified key, or the default value if the key is not present.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value
     * @param <T> the type of the value
     */
    public <T> T getOrDefault(String key, T defaultValue) {
        synchronized (this) {
            return (T) this.ctx.getOrDefault(key, defaultValue);
        }
    }

    /**
     * Clears all key-value pairs from this {@link Context}.
     */
    public void clear() {
        synchronized (this) {
            this.ctx.clear();
        }
    }

    /**
     * Returns all the KV entries in this {@link Context}.
     */
    public Set<Map.Entry<String, Object>> entrySet() {
        synchronized (this) {
            return this.ctx.entrySet();
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            return this.ctx.toString();
        }
    }

    @Override
    public Context copy() {
        synchronized (this) {
            Context copy = new Context();
            copy.ctx.putAll(this.ctx);
            return copy;
        }
    }
}
