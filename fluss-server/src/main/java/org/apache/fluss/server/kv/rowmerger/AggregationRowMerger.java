/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A merger that aggregates values per column using user-provided aggregation functions.
 * Supported functions: SUM, COUNT, LAST_VALUE, FIRST_VALUE, LISTAGG, MIN, MAX, PRODUCT, AVG.
 */
public class AggregationRowMerger implements RowMerger {

    /**
     * Aggregation functions supported by the aggregation merge engine.
     * Supported: SUM, COUNT, LAST_VALUE, FIRST_VALUE, LISTAGG, MIN, MAX, PRODUCT, AVG.
     */
    public enum AggFn {
        SUM,
        COUNT,
        LAST_VALUE,
        FIRST_VALUE,
        LISTAGG,
        MIN,
        MAX,
        PRODUCT,
        AVG;

        static AggFn fromString(String s) {
            String u = s.trim().toUpperCase();
            switch (u) {
                case "SUM":
                    return SUM;
                case "COUNT":
                    return COUNT;
                case "LAST_VALUE":
                case "LASTVALUE":
                    return LAST_VALUE;
                case "FIRST_VALUE":
                case "FIRSTVALUE":
                    return FIRST_VALUE;
                case "LISTAGG":
                    return LISTAGG;
                case "MIN":
                    return MIN;
                case "MAX":
                    return MAX;
                case "PRODUCT":
                case "PROD":
                    return PRODUCT;
                case "AVG":
                case "AVERAGE":
                    return AVG;
                default:
                    throw new IllegalArgumentException("Unsupported aggregation function: " + s);
            }
        }
    }

    private final RowType rowType;
    private final int[] primaryKeyIndexes;
    private final InternalRow.FieldGetter[] getters;
    private final RowEncoder encoder;
    private final Map<Integer, AggFn> aggByIndex; // only non-PK columns present

    public AggregationRowMerger(Schema schema, KvFormat kvFormat, Map<String, AggFn> aggByName) {
        this.rowType = schema.getRowType();
        this.primaryKeyIndexes = schema.getPrimaryKeyIndexes();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);
        this.getters = new InternalRow.FieldGetter[types.length];
        for (int i = 0; i < types.length; i++) {
            getters[i] = InternalRow.createFieldGetter(types[i], i);
        }
        this.encoder = RowEncoder.create(kvFormat, types);

        // translate name mapping to index mapping and validate
        this.aggByIndex = new HashMap<>();
        Set<Integer> pkSet = new HashSet<>();
        for (int pk : primaryKeyIndexes) {
            pkSet.add(pk);
        }
        for (Map.Entry<String, AggFn> e : aggByName.entrySet()) {
            int idx = rowType.getFieldIndex(e.getKey());
            checkArgument(
                    idx >= 0, "Aggregation function specified for unknown column '%s'", e.getKey());
            checkArgument(
                    !pkSet.contains(idx),
                    "Aggregation function must not be set on primary key column '%s'",
                    e.getKey());
            validateTypeCompatibility(rowType.getTypeAt(idx), e.getValue(), e.getKey());
            aggByIndex.put(idx, e.getValue());
        }
        // ensure all non-PK columns have functions
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (!pkSet.contains(i)) {
                checkState(
                        aggByIndex.containsKey(i),
                        "Aggregation function must be provided for non-primary-key column '%s'",
                        rowType.getFieldNames().get(i));
            }
        }
    }

    private void validateTypeCompatibility(DataType type, AggFn fn, String col) {
        DataTypeRoot root = type.getTypeRoot();
        switch (fn) {
            case SUM:
            case PRODUCT:
            case MIN:
            case MAX:
                // numeric types only
                checkArgument(
                        root == DataTypeRoot.INTEGER
                                || root == DataTypeRoot.BIGINT
                                || root == DataTypeRoot.FLOAT
                                || root == DataTypeRoot.DOUBLE,
                        "%s is only supported for numeric column '%s' of types [INT, BIGINT, FLOAT, DOUBLE], but is %s",
                        fn,
                        col,
                        type);
                break;
            case COUNT:
                // COUNT requires integer storage type
                checkArgument(
                        root == DataTypeRoot.INTEGER || root == DataTypeRoot.BIGINT,
                        "COUNT is only supported for integer column '%s' of types [INT, BIGINT], but is %s",
                        col,
                        type);
                break;
            case LISTAGG:
                checkArgument(
                        root == DataTypeRoot.STRING,
                        "LISTAGG is only supported for STRING column '%s', but is %s",
                        col,
                        type);
                break;
            case LAST_VALUE:
            case FIRST_VALUE:
                // any type allowed
                break;
            case AVG:
                // Temporarily restrict AVG until richer semantics are clarified.
                // We allow only FLOAT/DOUBLE to store running average; note that this is a limited implementation.
                checkArgument(
                        root == DataTypeRoot.FLOAT || root == DataTypeRoot.DOUBLE,
                        "AVG is only supported for FLOAT/DOUBLE column '%s', but is %s",
                        col,
                        type);
                break;
            default:
                throw new IllegalArgumentException("Unsupported function: " + fn);
        }
    }

    @Nullable
    @Override
    public BinaryRow merge(BinaryRow oldRow, BinaryRow newRow) {
        encoder.startNewRow();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (isPrimaryKey(i)) {
                // keep PK from oldRow
                encoder.encodeField(i, getters[i].getFieldOrNull(oldRow));
            } else {
                AggFn fn = aggByIndex.get(i);
                switch (fn) {
                    case SUM:
                        encodeSum(i, oldRow, newRow);
                        break;
                    case COUNT:
                        encodeCount(i, oldRow);
                        break;
                    case LAST_VALUE:
                        encodeLastValue(i, oldRow, newRow);
                        break;
                    case FIRST_VALUE:
                        encodeFirstValue(i, oldRow, newRow);
                        break;
                    case LISTAGG:
                        encodeListAgg(i, oldRow, newRow);
                        break;
                    case MIN:
                        encodeMin(i, oldRow, newRow);
                        break;
                    case MAX:
                        encodeMax(i, oldRow, newRow);
                        break;
                    case PRODUCT:
                        encodeProduct(i, oldRow, newRow);
                        break;
                    case AVG:
                        encodeAvg(i, oldRow, newRow);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported function: " + fn);
                }
            }
        }
        return encoder.finishRow();
    }

    private boolean isPrimaryKey(int index) {
        for (int pk : primaryKeyIndexes) {
            if (pk == index) {
                return true;
            }
        }
        return false;
    }

    private void encodeSum(int idx, InternalRow oldRow, InternalRow newRow) {
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case INTEGER:
                {
                    int a = oldRow.isNullAt(idx) ? 0 : oldRow.getInt(idx);
                    int b = newRow.isNullAt(idx) ? 0 : newRow.getInt(idx);
                    encoder.encodeField(idx, a + b);
                    break;
                }
            case BIGINT:
                {
                    long a = oldRow.isNullAt(idx) ? 0L : oldRow.getLong(idx);
                    long b = newRow.isNullAt(idx) ? 0L : newRow.getLong(idx);
                    encoder.encodeField(idx, a + b);
                    break;
                }
            case FLOAT:
                {
                    float a = oldRow.isNullAt(idx) ? 0F : oldRow.getFloat(idx);
                    float b = newRow.isNullAt(idx) ? 0F : newRow.getFloat(idx);
                    encoder.encodeField(idx, a + b);
                    break;
                }
            case DOUBLE:
                {
                    double a = oldRow.isNullAt(idx) ? 0D : oldRow.getDouble(idx);
                    double b = newRow.isNullAt(idx) ? 0D : newRow.getDouble(idx);
                    encoder.encodeField(idx, a + b);
                    break;
                }
            default:
                throw new IllegalStateException("SUM not supported for type: " + root);
        }
    }

    private void encodeCount(int idx, InternalRow oldRow) {
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case INTEGER:
                {
                    int a = oldRow.isNullAt(idx) ? 0 : oldRow.getInt(idx);
                    encoder.encodeField(idx, a + 1);
                    break;
                }
            case BIGINT:
                {
                    long a = oldRow.isNullAt(idx) ? 0L : oldRow.getLong(idx);
                    encoder.encodeField(idx, a + 1L);
                    break;
                }
            default:
                {
                    throw new IllegalStateException(
                            "COUNT must be stored in INT or BIGINT, but is: " + root);
                }
        }
    }

    @Nullable
    @Override
    public BinaryRow delete(BinaryRow oldRow) {
        throw new UnsupportedOperationException(
                "DELETE is not supported for the aggregation merge engine.");
    }

    @Override
    public boolean supportsDelete() {
        return false;
    }

    @Override
    public RowMerger configureTargetColumns(@Nullable int[] targetColumns) {
        if (targetColumns == null) {
            return this;
        }
        throw new UnsupportedOperationException(
                "Partial update is not supported for the aggregation merge engine.");
    }

    private void encodeLastValue(int idx, InternalRow oldRow, InternalRow newRow) {
        Object newVal = getters[idx].getFieldOrNull(newRow);
        if (newVal != null) {
            encoder.encodeField(idx, newVal);
        } else {
            encoder.encodeField(idx, getters[idx].getFieldOrNull(oldRow));
        }
    }

    private void encodeFirstValue(int idx, InternalRow oldRow, InternalRow newRow) {
        Object oldVal = getters[idx].getFieldOrNull(oldRow);
        if (oldVal != null) {
            encoder.encodeField(idx, oldVal);
        } else {
            encoder.encodeField(idx, getters[idx].getFieldOrNull(newRow));
        }
    }

    private void encodeListAgg(int idx, InternalRow oldRow, InternalRow newRow) {
        String oldStr = null;
        if (!oldRow.isNullAt(idx)) {
            Object o = getters[idx].getFieldOrNull(oldRow);
            oldStr = (o == null) ? null : o.toString();
        }
        String newStr = null;
        if (!newRow.isNullAt(idx)) {
            Object n = getters[idx].getFieldOrNull(newRow);
            newStr = (n == null) ? null : n.toString();
        }
        String res;
        if (oldStr == null) {
            res = newStr;
        } else if (newStr == null || newStr.isEmpty()) {
            res = oldStr;
        } else if (oldStr.isEmpty()) {
            res = newStr;
        } else {
            res = oldStr + "," + newStr;
        }
        if (res == null) {
            encoder.encodeField(idx, null);
        } else {
            encoder.encodeField(idx, org.apache.fluss.row.BinaryString.fromString(res));
        }
    }

    private void encodeMin(int idx, InternalRow oldRow, InternalRow newRow) {
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case INTEGER: {
                Integer a = oldRow.isNullAt(idx) ? null : oldRow.getInt(idx);
                Integer b = newRow.isNullAt(idx) ? null : newRow.getInt(idx);
                Integer r = (a == null) ? b : (b == null ? a : Math.min(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case BIGINT: {
                Long a = oldRow.isNullAt(idx) ? null : oldRow.getLong(idx);
                Long b = newRow.isNullAt(idx) ? null : newRow.getLong(idx);
                Long r = (a == null) ? b : (b == null ? a : Math.min(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case FLOAT: {
                Float a = oldRow.isNullAt(idx) ? null : oldRow.getFloat(idx);
                Float b = newRow.isNullAt(idx) ? null : newRow.getFloat(idx);
                Float r = (a == null) ? b : (b == null ? a : Math.min(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case DOUBLE: {
                Double a = oldRow.isNullAt(idx) ? null : oldRow.getDouble(idx);
                Double b = newRow.isNullAt(idx) ? null : newRow.getDouble(idx);
                Double r = (a == null) ? b : (b == null ? a : Math.min(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            default:
                throw new IllegalStateException("MIN not supported for type: " + root);
        }
    }

    private void encodeMax(int idx, InternalRow oldRow, InternalRow newRow) {
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case INTEGER: {
                Integer a = oldRow.isNullAt(idx) ? null : oldRow.getInt(idx);
                Integer b = newRow.isNullAt(idx) ? null : newRow.getInt(idx);
                Integer r = (a == null) ? b : (b == null ? a : Math.max(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case BIGINT: {
                Long a = oldRow.isNullAt(idx) ? null : oldRow.getLong(idx);
                Long b = newRow.isNullAt(idx) ? null : newRow.getLong(idx);
                Long r = (a == null) ? b : (b == null ? a : Math.max(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case FLOAT: {
                Float a = oldRow.isNullAt(idx) ? null : oldRow.getFloat(idx);
                Float b = newRow.isNullAt(idx) ? null : newRow.getFloat(idx);
                Float r = (a == null) ? b : (b == null ? a : Math.max(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            case DOUBLE: {
                Double a = oldRow.isNullAt(idx) ? null : oldRow.getDouble(idx);
                Double b = newRow.isNullAt(idx) ? null : newRow.getDouble(idx);
                Double r = (a == null) ? b : (b == null ? a : Math.max(a, b));
                encoder.encodeField(idx, r);
                break;
            }
            default:
                throw new IllegalStateException("MAX not supported for type: " + root);
        }
    }

    private void encodeProduct(int idx, InternalRow oldRow, InternalRow newRow) {
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case INTEGER: {
                Integer a = oldRow.isNullAt(idx) ? null : oldRow.getInt(idx);
                Integer b = newRow.isNullAt(idx) ? null : newRow.getInt(idx);
                Integer r = (a == null) ? b : (b == null ? a : a * b);
                encoder.encodeField(idx, r);
                break;
            }
            case BIGINT: {
                Long a = oldRow.isNullAt(idx) ? null : oldRow.getLong(idx);
                Long b = newRow.isNullAt(idx) ? null : newRow.getLong(idx);
                Long r = (a == null) ? b : (b == null ? a : a * b);
                encoder.encodeField(idx, r);
                break;
            }
            case FLOAT: {
                Float a = oldRow.isNullAt(idx) ? null : oldRow.getFloat(idx);
                Float b = newRow.isNullAt(idx) ? null : newRow.getFloat(idx);
                Float r = (a == null) ? b : (b == null ? a : a * b);
                encoder.encodeField(idx, r);
                break;
            }
            case DOUBLE: {
                Double a = oldRow.isNullAt(idx) ? null : oldRow.getDouble(idx);
                Double b = newRow.isNullAt(idx) ? null : newRow.getDouble(idx);
                Double r = (a == null) ? b : (b == null ? a : a * b);
                encoder.encodeField(idx, r);
                break;
            }
            default:
                throw new IllegalStateException("PRODUCT not supported for type: " + root);
        }
    }

    private void encodeAvg(int idx, InternalRow oldRow, InternalRow newRow) {
        // Limited running average without explicit count state: if either side is null, take the other; otherwise average the two.
        // Note: This is a simplified behavior pending clarified AVG semantics with persistent count.
        DataTypeRoot root = rowType.getTypeAt(idx).getTypeRoot();
        switch (root) {
            case FLOAT: {
                Float a = oldRow.isNullAt(idx) ? null : oldRow.getFloat(idx);
                Float b = newRow.isNullAt(idx) ? null : newRow.getFloat(idx);
                Float r;
                if (a == null) r = b;
                else if (b == null) r = a;
                else r = (a + b) / 2.0f;
                encoder.encodeField(idx, r);
                break;
            }
            case DOUBLE: {
                Double a = oldRow.isNullAt(idx) ? null : oldRow.getDouble(idx);
                Double b = newRow.isNullAt(idx) ? null : newRow.getDouble(idx);
                Double r;
                if (a == null) r = b;
                else if (b == null) r = a;
                else r = (a + b) / 2.0d;
                encoder.encodeField(idx, r);
                break;
            }
            default:
                throw new IllegalStateException("AVG not supported for type: " + root);
        }
    }
}
