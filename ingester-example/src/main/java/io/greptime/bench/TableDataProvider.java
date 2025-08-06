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

package io.greptime.bench;

import io.greptime.models.TableSchema;
import java.util.Iterator;

/**
 * The benchmark table data provider.
 */
public interface TableDataProvider extends AutoCloseable {

    /**
     * Initialize the table data provider. Maybe do some pre-processing here.
     */
    void init();

    /**
     * Returns the table schema.
     */
    TableSchema tableSchema();

    /**
     * Returns the iterator of the rows.
     */
    Iterator<Object[]> rows();

    /**
     * Returns the total number of rows.
     */
    long rowCount();
}
