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

package io.greptime.common.util;

/**
 * An object pool.
 */
public interface ObjectPool<T> {

    /**
     * Gets an object from the pool.
     *
     * @return an object from the pool
     */
    T getObject();

    /**
     * Returns the object to the pool. The caller should not use the object beyond this point.
     *
     * @param returned the object to return to the pool
     */
    void returnObject(T returned);

    /**
     * Defines a resource, and the way to create and destroy instances of it.
     *
     * @param <T> the type of the resource
     */
    interface Resource<T> {

        /**
         * Creates a new instance of the resource.
         *
         * @return a new instance of the resource
         */
        T create();

        /**
         * Destroys the given instance.
         *
         * @param instance the instance to destroy
         */
        void close(T instance);
    }
}
