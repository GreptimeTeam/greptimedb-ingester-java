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

import io.greptime.common.util.Ensures;
import io.greptime.errors.PojoException;
import io.greptime.models.Column;
import io.greptime.models.DataType;
import io.greptime.models.Metric;
import io.greptime.models.SemanticType;
import io.greptime.models.TableRows;
import io.greptime.models.TableSchema;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This utility class converts POJO classes into {@link io.greptime.models.TableRows} objects,
 * inspired by <a href="https://github.com/influxdata/influxdb-client-java/blob/master/client/src/main/java/com/influxdb/client/internal/MeasurementMapper.java">InfluxDB client-java</a>.
 *
 * @author jiachun.fjc
 */
public class PojoMapper {

    private final ConcurrentMap<String, Map<String, Field>> classFieldCache = new ConcurrentHashMap<>();
    private final int maxCachedPOJOs;

    public PojoMapper(int maxCachedPOJOs) {
        this.maxCachedPOJOs = maxCachedPOJOs;
    }

    public <M> TableRows toTableRows(List<M> pojos) {
        Ensures.ensureNonNull(pojos, "pojos");
        Ensures.ensure(!pojos.isEmpty(), "pojos can not be empty");

        M first = pojos.get(0);

        Class<?> metricType = first.getClass();

        Map<String, Field> fieldMap = getAndCacheMetricClass(metricType);

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

        TableRows tableRows = TableRows.from(schemaBuilder.build());
        for (M pojo : pojos) {
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
            tableRows.insert(values);
        }

        return tableRows;
    }

    private String getMetricName(Class<?> metricType) {
        // From @Metric annotation
        Metric metricAnnotation = metricType.getAnnotation(Metric.class);
        if (metricAnnotation != null) {
            return metricAnnotation.name();
        } else {
            String err =
                    String.format("Unable to determine Metric for '%s'." + " Does it have a @Metric annotation?",
                            metricType);
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

    private Map<String, Field> getAndCacheMetricClass(Class<?> metricType) {
        if (this.classFieldCache.size() >= this.maxCachedPOJOs) {
            this.classFieldCache.clear();
        }

        return this.classFieldCache.computeIfAbsent(metricType.getName(), k -> {
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
        });
    }
}
