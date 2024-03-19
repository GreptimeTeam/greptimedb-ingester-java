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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.greptime.common.SPI;
import io.greptime.common.util.Ensures;
import io.greptime.errors.PojoException;
import io.greptime.models.Column;
import io.greptime.models.DataType;
import io.greptime.models.Metric;
import io.greptime.models.SemanticType;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This utility class converts POJO objects into {@link Table} objects,
 * inspired by <a href="https://github.com/influxdata/influxdb-client-java/blob/master/client/src/main/java/com/influxdb/client/internal/MeasurementMapper.java">InfluxDB client-java</a>.
 */
@SPI(priority = 9)
public class CachedPojoObjectMapper implements PojoObjectMapper {

    private final LoadingCache<Class<?>, Map<String, Field>> classFieldCache;

    public CachedPojoObjectMapper() {
        this(1024);
    }

    public CachedPojoObjectMapper(int maxCachedPOJOs) {
        this.classFieldCache = CacheBuilder.newBuilder()
                .maximumSize(maxCachedPOJOs)
                .build(new CacheLoader<Class<?>, Map<String, Field>>() {
                    @SuppressWarnings("NullableProblems")
                    @Override
                    public Map<String, Field> load(Class<?> key) {
                        return createMetricClass(key);
                    }
                });
    }

    @Override
    public <M> Table mapToTable(List<M> pojoObjects) {
        Ensures.ensureNonNull(pojoObjects, "pojoObjects");
        Ensures.ensure(!pojoObjects.isEmpty(), "pojoObjects can not be empty");

        M first = pojoObjects.get(0);

        Class<?> metricType = first.getClass();

        Map<String, Field> fieldMap = this.classFieldCache.getUnchecked(metricType);

        String metricName = getMetricName(metricType);

        TableSchema.Builder schemaBuilder = TableSchema.newBuilder(metricName);
        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            String name = entry.getKey();
            Field field = entry.getValue();
            Column column = field.getAnnotation(Column.class);
            DataType dataType = column.dataType();
            SemanticType semanticType = SemanticType.Field;
            if (column.tag()) {
                semanticType = SemanticType.Tag;
            } else if (column.timestamp()) {
                semanticType = SemanticType.Timestamp;
            }
            schemaBuilder.addColumn(name, semanticType, dataType);
        }

        Table table = Table.from(schemaBuilder.build());
        for (M pojo : pojoObjects) {
            Class<?> type = pojo.getClass();
            if (!type.equals(metricType)) {
                throw new PojoException("All POJOs must be of the same type");
            }

            Object[] values = new Object[fieldMap.size()];
            int j = 0;
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
                Field field = entry.getValue();
                Object value = getObject(pojo, field);
                values[j] = value;

                j++;
            }
            table.addRow(values);
        }

        return table;
    }

    private String getMetricName(Class<?> metricType) {
        // From @Metric annotation
        Metric metricAnnotation = metricType.getAnnotation(Metric.class);
        if (metricAnnotation != null) {
            return metricAnnotation.name();
        } else {
            String err = String.format(
                    "Unable to determine Metric for '%s'." + " Does it have a @Metric annotation?", metricType);
            throw new PojoException(err);
        }
    }

    private <M> Object getObject(M metric, Field field) {
        Object value;
        try {
            field.setAccessible(true);
            value = field.get(metric);
        } catch (IllegalAccessException e) {
            throw new PojoException(e);
        }
        return value;
    }

    private Map<String, Field> createMetricClass(Class<?> metricType) {
        Map<String, Field> fieldMap = new HashMap<>();
        Class<?> currentType = metricType;
        while (currentType != null) {
            for (Field field : currentType.getDeclaredFields()) {
                Column colAnnotation = field.getAnnotation(Column.class);
                if (colAnnotation == null) {
                    continue;
                }
                fieldMap.put(colAnnotation.name(), field);
            }
            currentType = currentType.getSuperclass();
        }
        return fieldMap;
    }
}
