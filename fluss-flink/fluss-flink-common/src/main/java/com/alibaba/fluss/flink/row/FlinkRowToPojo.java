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
import java.lang.reflect.Modifier;
import java.util.Arrays;

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
            Field[] allFields = pojoClass.getDeclaredFields();

            // Filter out synthetic fields (JaCoCo's $jacocoData)
            Field[] fields =
                    Arrays.stream(allFields)
                            .filter(
                                    field ->
                                            !field.isSynthetic()
                                                    && !Modifier.isStatic(field.getModifiers()))
                            .toArray(Field[]::new);

            if (row.getArity() != fields.length) {
                throw new IllegalArgumentException(
                        "Row arity ("
                                + row.getArity()
                                + ") does not match the number of fields ("
                                + fields.length
                                + ") in the POJO class: "
                                + pojoClass.getName());
            }

            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                field.setAccessible(true);
                Object value = row.getField(i);

                // Handle null values for primitive types
                if (value == null && field.getType().isPrimitive()) {
                    if (field.getType() == int.class) {
                        value = 0;
                    } else if (field.getType() == long.class) {
                        value = 0L;
                    } else if (field.getType() == double.class) {
                        value = 0.0;
                    } else if (field.getType() == float.class) {
                        value = 0.0f;
                    } else if (field.getType() == short.class) {
                        value = (short) 0;
                    } else if (field.getType() == byte.class) {
                        value = (byte) 0;
                    } else if (field.getType() == boolean.class) {
                        value = false;
                    } else if (field.getType() == char.class) {
                        value = '\u0000';
                    }
                }

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
                    } else if (field.getType() == Boolean.class
                            || field.getType() == boolean.class) {
                        if (value instanceof Number) {
                            value = ((Number) value).intValue() != 0;
                        } else {
                            value = Boolean.parseBoolean(value.toString());
                        }
                    } else if (field.getType() == String.class) {
                        // Convert any type to String if needed
                        value = value.toString();
                    } else if (field.getType() == java.time.LocalDateTime.class) {
                        // Handle TimestampNtz to LocalDateTime conversion
                        if (value.getClass()
                                .getName()
                                .equals("com.alibaba.fluss.row.TimestampNtz")) {
                            try {
                                // Try invoking toLocalDateTime() method if it exists
                                java.lang.reflect.Method method =
                                        value.getClass().getMethod("toLocalDateTime");
                                value = method.invoke(value);
                            } catch (NoSuchMethodException e) {
                                // Fallback 1: try getting internal value
                                try {
                                    java.lang.reflect.Method method =
                                            value.getClass().getMethod("getMillisecond");
                                    Long milliseconds = (Long) method.invoke(value);
                                    value =
                                            java.time.Instant.ofEpochMilli(milliseconds)
                                                    .atZone(java.time.ZoneId.systemDefault())
                                                    .toLocalDateTime();
                                } catch (Exception ex) {
                                    // Fallback 2: try parsing the string representation
                                    String strValue = value.toString().trim().replace(" ", "T");
                                    value = java.time.LocalDateTime.parse(strValue);
                                }
                            }
                        } else if (value instanceof Long) {
                            // Convert epoch millis to LocalDateTime
                            value =
                                    java.time.Instant.ofEpochMilli((Long) value)
                                            .atZone(java.time.ZoneId.systemDefault())
                                            .toLocalDateTime();
                        }
                    } else if (field.getType() == java.time.LocalDate.class) {
                        if (value instanceof java.time.LocalDateTime) {
                            value = ((java.time.LocalDateTime) value).toLocalDate();
                        } else {
                            // Try to convert string to LocalDate
                            value = java.time.LocalDate.parse(value.toString());
                        }
                    } else if (field.getType() == java.util.Date.class) {
                        if (value instanceof java.time.LocalDateTime) {
                            value = java.sql.Timestamp.valueOf((java.time.LocalDateTime) value);
                        } else if (value instanceof Long) {
                            value = new java.util.Date((Long) value);
                        }
                    }
                }

                field.set(pojo, value);
            }

            return pojo;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error converting Flink Row to POJO. Row: "
                            + row
                            + ", POJO class: "
                            + pojoClass.getName(),
                    e);
        }
    }
}
