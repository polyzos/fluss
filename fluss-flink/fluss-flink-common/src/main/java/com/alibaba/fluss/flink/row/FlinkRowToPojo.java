/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.row;

import org.apache.flink.types.Row;

import java.lang.reflect.Field;

/**
 * Helper class for converting a Flink Row to a POJO using reflection
 *
 * <p>Note: Can be used for testing on the datastream source api and users can use it if they wish
 * to convert Flink rows to POJOs.
 */
public class FlinkRowToPojo {

    public static <T> T convert(Row row, Class<T> pojoClass) {
        try {
            T pojo = pojoClass.getDeclaredConstructor().newInstance();
            Field[] fields = pojoClass.getDeclaredFields();

            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object value = row.getField(i);

                // Convert types if necessary
                if (value != null && !field.getType().isAssignableFrom(value.getClass())) {
                    if (field.getType() == Integer.class || field.getType() == int.class) {
                        value = ((Number) value).intValue();
                    } else if (field.getType() == Long.class || field.getType() == long.class) {
                        value = ((Number) value).longValue();
                    } else if (field.getType() == Double.class || field.getType() == double.class) {
                        value = ((Number) value).doubleValue();
                    } else if (field.getType() == Float.class || field.getType() == float.class) {
                        value = ((Number) value).floatValue();
                    } else if (field.getType() == Short.class || field.getType() == short.class) {
                        value = ((Number) value).shortValue();
                    } else if (field.getType() == Byte.class || field.getType() == byte.class) {
                        value = ((Number) value).byteValue();
                    }
                }

                field.set(pojo, value);
            }

            return pojo;
        } catch (Exception e) {
            throw new RuntimeException("Error converting Flink Row to POJO", e);
        }
    }
}
