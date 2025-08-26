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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A merging interface defines how to merge a new row with existing row. */
public interface RowMerger {

    /**
     * Merge the old row with the new row.
     *
     * @param oldRow the old row
     * @param newRow the new row
     * @return the merged row, if the returned row is the same to the old row, then nothing happens
     *     to the row (no update, no delete).
     */
    BinaryRow merge(BinaryRow oldRow, BinaryRow newRow);

    /**
     * Merge the old row with a delete row.
     *
     * <p>This method will be invoked only when {@link #supportsDelete()} returns true.
     *
     * @param oldRow the old row.
     * @return the merged row, or null if the row is deleted.
     */
    @Nullable
    BinaryRow delete(BinaryRow oldRow);

    /**
     * Whether the merger supports to merge delete rows.
     *
     * @return true if the merger supports delete operation.
     */
    boolean supportsDelete();

    /** Dynamically configure the target columns to merge and return the effective merger. */
    RowMerger configureTargetColumns(@Nullable int[] targetColumns);

    /** Create a row merger based on the given configuration. */
    static RowMerger create(TableConfig tableConf, Schema schema, KvFormat kvFormat) {
        Optional<MergeEngineType> mergeEngineType = tableConf.getMergeEngineType();
        if (mergeEngineType.isPresent()) {
            switch (mergeEngineType.get()) {
                case FIRST_ROW:
                    return new FirstRowRowMerger();
                case VERSIONED:
                    Optional<String> versionColumn = tableConf.getMergeEngineVersionColumn();
                    if (!versionColumn.isPresent()) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "'%s' must be set for versioned merge engine.",
                                        ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                    }
                    return new VersionedRowMerger(schema.getRowType(), versionColumn.get());
                case AGGREGATION:
                    // Prefer per-field configuration: fields.<field>.aggregation-function
                    Map<String, String> perField = tableConf.getFieldAggregationFunctions();
                    Map<String, AggregationRowMerger.AggFn> aggByName = new HashMap<>();
                    if (!perField.isEmpty()) {
                        for (Map.Entry<String, String> e : perField.entrySet()) {
                            aggByName.put(e.getKey(), AggregationRowMerger.AggFn.fromString(e.getValue()));
                        }
                    } else {
                        // Fallback to legacy combined option
                        Optional<String> aggFuncsOpt = tableConf.getMergeEngineAggregationFunctions();
                        if (!aggFuncsOpt.isPresent() || aggFuncsOpt.get().trim().isEmpty()) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "Either per-field aggregation options 'fields.<col>.aggregation-function' must be set, or legacy '%s' must be provided for aggregation merge engine.",
                                            ConfigOptions.TABLE_MERGE_ENGINE_AGGREGATION_FUNCTIONS.key()));
                        }
                        aggByName = parseAggFunctions(aggFuncsOpt.get());
                    }
                    return new AggregationRowMerger(schema, kvFormat, aggByName);
                default:
                    throw new IllegalArgumentException(
                            "Unsupported merge engine type: " + mergeEngineType.get());
            }
        } else {
            return new DefaultRowMerger(schema, kvFormat);
        }
    }

    /** Parse aggregation functions string with format: "col1:sum,col2:count" (case-insensitive). */
    static Map<String, AggregationRowMerger.AggFn> parseAggFunctions(String funcs) {
        Map<String, AggregationRowMerger.AggFn> map = new HashMap<>();
        String[] parts = funcs.split(",");
        for (String p : parts) {
            if (p == null || p.trim().isEmpty()) {
                continue;
            }
            String[] kv = p.split(":");
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid aggregation function entry: '"
                                + p
                                + "'. Expected format 'col:func'.");
            }
            String col = kv[0].trim();
            String fn = kv[1].trim();
            if (col.isEmpty() || fn.isEmpty()) {
                throw new IllegalArgumentException(
                        "Invalid aggregation function entry: '"
                                + p
                                + "'. Column or function is empty.");
            }
            map.put(col, AggregationRowMerger.AggFn.fromString(fn));
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("No valid aggregation functions specified.");
        }
        return map;
    }
}
