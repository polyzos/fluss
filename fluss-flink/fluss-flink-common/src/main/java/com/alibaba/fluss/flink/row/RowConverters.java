/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.row;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

public class RowConverters {

    /**
     * Converts a POJO to Flink RowData using the specified RowType schema.
     *
     * <p>This method maps fields from the POJO to the RowData structure according to field names in
     * the provided RowType. It handles type conversions to ensure compatibility with Flink's type
     * system.
     *
     * <p>Fields that exist in the RowType but not in the POJO will be set to null.
     *
     * @param pojo The POJO to convert (can be null, resulting in null return)
     * @param rowType The Flink RowType describing the expected row structure
     * @return The converted RowData object, or null if input is null
     * @throws RuntimeException if conversion fails for any field
     */
    public static RowData convertToRowData(Object pojo, RowType rowType) {
        if (pojo == null) {
            return null;
        }

        List<RowType.RowField> fields = rowType.getFields();
        int fieldCount = fields.size();
        GenericRowData rowData = new GenericRowData(fieldCount);

        Class<?> pojoClass = pojo.getClass();

        for (int i = 0; i < fieldCount; i++) {
            String fieldName = fields.get(i).getName();
            LogicalType fieldType = fields.get(i).getType();

            try {
                Field field = findField(pojoClass, fieldName);
                if (field == null) {
                    rowData.setField(i, null);
                    continue;
                }

                field.setAccessible(true);
                Object value = field.get(pojo);

                if (value == null) {
                    rowData.setField(i, null);
                } else {
                    rowData.setField(i, convertToRowDataField(value, fieldType));
                }
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to convert field '" + fieldName + "' to RowData", e);
            }
        }

        return rowData;
    }

    /** Finds a field in a class or its superclasses. */
    private static Field findField(Class<?> clazz, String fieldName) {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            if (clazz.getSuperclass() != null) {
                return findField(clazz.getSuperclass(), fieldName);
            }
            return null;
        }
    }

    /** Converts a single field value to the appropriate RowData format. */
    private static Object convertToRowDataField(Object value, LogicalType fieldType) {
        if (value == null) {
            return null;
        }

        LogicalTypeRoot typeRoot = fieldType.getTypeRoot();

        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return convertToStringData(value);

            case BOOLEAN:
                return value instanceof Boolean
                        ? (Boolean) value
                        : Boolean.parseBoolean(value.toString());

            case TINYINT:
                if (value instanceof Byte) {
                    return (Byte) value;
                }
                return Byte.parseByte(value.toString());

            case SMALLINT:
                if (value instanceof Short) {
                    return (Short) value;
                }
                return Short.parseShort(value.toString());

            case INTEGER:
                if (value instanceof Integer) {
                    return (Integer) value;
                }
                return Integer.parseInt(value.toString());

            case BIGINT:
                if (value instanceof Long) {
                    return (Long) value;
                }
                return Long.parseLong(value.toString());

            case FLOAT:
                if (value instanceof Float) {
                    return (Float) value;
                }
                return Float.parseFloat(value.toString());

            case DOUBLE:
                if (value instanceof Double) {
                    return (Double) value;
                }
                return Double.parseDouble(value.toString());

            case DECIMAL:
                if (value instanceof BigDecimal) {
                    return value;
                }
                return new BigDecimal(value.toString());

            case DATE:
                return convertToDateInt(value);

            case TIME_WITHOUT_TIME_ZONE:
                return convertToTimeInt(value);

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertToTimestampData(value);

                // Add more type conversions if needed

            default:
                throw new UnsupportedOperationException(
                        "Unsupported conversion to RowData for type: " + fieldType);
        }
    }

    /** Converts various string-like types to StringData. */
    private static StringData convertToStringData(Object value) {
        if (value instanceof StringData) {
            return (StringData) value;
        }
        return StringData.fromString(value.toString());
    }

    /** Converts various date types to days since epoch (int). */
    private static int convertToDateInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }

        LocalDate localDate;
        if (value instanceof LocalDate) {
            localDate = (LocalDate) value;
        } else if (value instanceof LocalDateTime) {
            localDate = ((LocalDateTime) value).toLocalDate();
        } else if (value instanceof Date) {
            localDate = ((Date) value).toLocalDate();
        } else if (value instanceof java.util.Date) {
            localDate =
                    Instant.ofEpochMilli(((java.util.Date) value).getTime())
                            .atZone(java.time.ZoneId.systemDefault())
                            .toLocalDate();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot convert " + value.getClass().getName() + " to Date");
        }

        return (int) localDate.toEpochDay();
    }

    /** Converts various time types to milliseconds since midnight (int). */
    private static int convertToTimeInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }

        LocalTime localTime;
        if (value instanceof LocalTime) {
            localTime = (LocalTime) value;
        } else if (value instanceof LocalDateTime) {
            localTime = ((LocalDateTime) value).toLocalTime();
        } else if (value instanceof Time) {
            localTime = ((Time) value).toLocalTime();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot convert " + value.getClass().getName() + " to Time");
        }

        return localTime.toSecondOfDay() * 1000;
    }

    /** Converts various timestamp types to TimestampData. */
    private static TimestampData convertToTimestampData(Object value) {
        if (value instanceof TimestampData) {
            return (TimestampData) value;
        }

        if (value instanceof Timestamp) {
            Timestamp ts = (Timestamp) value;
            return TimestampData.fromTimestamp(ts);
        }

        if (value instanceof LocalDateTime) {
            LocalDateTime ldt = (LocalDateTime) value;
            return TimestampData.fromLocalDateTime(ldt);
        }

        if (value instanceof java.util.Date) {
            long millis = ((java.util.Date) value).getTime();
            return TimestampData.fromEpochMillis(millis);
        }

        if (value instanceof Instant) {
            Instant instant = (Instant) value;
            return TimestampData.fromInstant(instant);
        }

        throw new UnsupportedOperationException(
                "Cannot convert " + value.getClass().getName() + " to TimestampData");
    }

    /**
     * Converts a POJO to Flink RowData by automatically inferring the schema.
     *
     * <p>This method first analyzes the POJO class structure to determine an appropriate RowType,
     * then performs the conversion based on the inferred type information.
     *
     * @param pojo The POJO object to convert
     * @return The converted RowData, or null if input is null
     */
    public static RowData pojoToRowData(Object pojo) {
        if (pojo == null) {
            return null;
        }

        RowType rowType = inferRowTypeFromClass(pojo.getClass());
        return convertToRowData(pojo, rowType);
    }

    /**
     * Infers a Flink RowType from a POJO class using reflection.
     *
     * <p>This method analyzes the fields of the provided class and its superclasses, mapping Java
     * types to appropriate Flink LogicalType instances. It skips static and transient fields.
     *
     * <p>For complex types not directly supported, it falls back to VarCharType.
     *
     * @param pojoClass The class to analyze
     * @return The inferred RowType representing the structure of the class
     */
    public static RowType inferRowTypeFromClass(Class<?> pojoClass) {
        List<Field> allFields = getAllFields(pojoClass);
        List<RowType.RowField> rowFields = new ArrayList<>(allFields.size());

        for (Field field : allFields) {
            String fieldName = field.getName();
            LogicalType logicalType = inferLogicalTypeFromField(field);
            rowFields.add(new RowType.RowField(fieldName, logicalType));
        }

        return new RowType(false, rowFields);
    }

    /** Gets all fields from a class and its superclasses. */
    private static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();

        // Get fields from this class and all superclasses
        Class<?> currentClass = clazz;
        while (currentClass != null && currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                // Skip static and transient fields
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())
                        || java.lang.reflect.Modifier.isTransient(field.getModifiers())) {
                    continue;
                }
                fields.add(field);
            }
            currentClass = currentClass.getSuperclass();
        }

        return fields;
    }

    /** Infers a LogicalType from a field's Java type. */
    private static LogicalType inferLogicalTypeFromField(Field field) {
        Class<?> type = field.getType();

        // Handle primitive types
        if (type == Boolean.class || type == boolean.class) {
            return new org.apache.flink.table.types.logical.BooleanType(type.isPrimitive());
        } else if (type == Byte.class || type == byte.class) {
            return new org.apache.flink.table.types.logical.TinyIntType(type.isPrimitive());
        } else if (type == Short.class || type == short.class) {
            return new org.apache.flink.table.types.logical.SmallIntType(type.isPrimitive());
        } else if (type == Integer.class || type == int.class) {
            return new org.apache.flink.table.types.logical.IntType(type.isPrimitive());
        } else if (type == Long.class || type == long.class) {
            return new org.apache.flink.table.types.logical.BigIntType(type.isPrimitive());
        } else if (type == Float.class || type == float.class) {
            return new org.apache.flink.table.types.logical.FloatType(type.isPrimitive());
        } else if (type == Double.class || type == double.class) {
            return new org.apache.flink.table.types.logical.DoubleType(type.isPrimitive());
        }

        // Handle common object types
        else if (type == String.class) {
            return new org.apache.flink.table.types.logical.VarCharType(
                    false, VarCharType.MAX_LENGTH);
        } else if (type == BigDecimal.class) {
            return new org.apache.flink.table.types.logical.DecimalType(false, 38, 18);
        } else if (type == java.sql.Date.class || type == LocalDate.class) {
            return new org.apache.flink.table.types.logical.DateType(false);
        } else if (type == java.sql.Time.class || type == LocalTime.class) {
            return new org.apache.flink.table.types.logical.TimeType(false, 0);
        } else if (type == java.sql.Timestamp.class
                || type == java.util.Date.class
                || type == LocalDateTime.class
                || type == Instant.class) {
            return new org.apache.flink.table.types.logical.TimestampType(false, 3);
        }

        // Arrays could be handled as ArrayType with component type inference
        // Maps could be handled as MapType with key/value type inference
        // We'll use VARCHAR as fallback for unsupported types
        else {
            return new org.apache.flink.table.types.logical.VarCharType(
                    false, VarCharType.MAX_LENGTH);
        }
    }
}
