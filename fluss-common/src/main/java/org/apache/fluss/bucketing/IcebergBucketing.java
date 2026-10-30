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

package org.apache.fluss.bucketing;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The bucketing contract of Iceberg-format tables, shared by the key encoders, the server-side
 * validation and the Iceberg lake catalog.
 *
 * <p>Iceberg's {@code bucket[N]} transform takes a single column, and the Iceberg table is
 * partitioned by {@code bucket[N]} of that column so that Fluss bucket {@code i} is exactly Iceberg
 * bucket {@code i}. A bucket key may still have several columns: all of them form the lookup index
 * (prefix lookups and delta joins), but only the first one ({@link #hashKey}) decides the bucket.
 *
 * <p>Because hashing only the first column brings no benefit unless the extra columns narrow a
 * prefix lookup, a multi-column bucket key is only accepted in the shape where that lookup exists
 * (see {@link #validateBucketKeys(List, List, Integer)}). This also guarantees that a default
 * bucket key (equal to the physical primary key) of an Iceberg table always has exactly one column,
 * which is what allows callers to reuse the primary key encoder for bucketing.
 */
@Internal
public final class IcebergBucketing {

    private IcebergBucketing() {}

    /**
     * Returns the column hashed by Iceberg's single-argument {@code bucket[N]} transform: the first
     * bucket-key column.
     */
    public static String hashKey(List<String> bucketKeys) {
        checkArgument(!bucketKeys.isEmpty(), "Bucket keys must not be empty.");
        return bucketKeys.get(0);
    }

    /**
     * Validates that the bucket keys of an Iceberg-format table are supported, see {@link
     * #validateBucketKeys(List, List, Integer)}.
     */
    public static void validateBucketKeys(TableDescriptor tableDescriptor) {
        List<String> physicalPrimaryKeys = new ArrayList<>();
        if (tableDescriptor.hasPrimaryKey()) {
            physicalPrimaryKeys.addAll(
                    tableDescriptor.getSchema().getPrimaryKey().get().getColumnNames());
            physicalPrimaryKeys.removeAll(tableDescriptor.getPartitionKeys());
        }
        validateBucketKeys(
                tableDescriptor.getBucketKeys(),
                physicalPrimaryKeys,
                Configuration.fromMap(tableDescriptor.getProperties())
                        .getOptional(ConfigOptions.TABLE_KV_FORMAT_VERSION)
                        .orElse(null));
    }

    /**
     * Validates that the bucket keys of a table are supported if it is an Iceberg-format table, see
     * {@link #validateBucketKeys(List, List, Integer)}. This is cheap for tables with at most one
     * bucket key column, so it can run on the write path.
     *
     * <p>Tablet servers check this on writes because new tables are only validated at creation.
     * Tables created before that validation existed used to be unwritable, since the Iceberg key
     * encoder rejected multi-column keys, but would otherwise become writable now that the Iceberg
     * bucket key encoder only encodes the first bucket key column.
     */
    public static void validateBucketKeys(TableInfo tableInfo) {
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (bucketKeys.size() <= 1
                || tableInfo.getTableConfig().getDataLakeFormat().orElse(null)
                        != DataLakeFormat.ICEBERG) {
            return;
        }
        validateBucketKeys(
                bucketKeys,
                tableInfo.hasPrimaryKey()
                        ? tableInfo.getPhysicalPrimaryKeys()
                        : Collections.emptyList(),
                tableInfo.getTableConfig().getKvFormatVersion().orElse(null));
    }

    /**
     * Validates that the bucket keys of an Iceberg-format table are supported.
     *
     * <p>A bucket key with more than one column is only supported when all of the following hold:
     *
     * <ul>
     *   <li>the table is a primary key table, since a log table has no lookup index and would only
     *       be distributed by the first column;
     *   <li>the table uses kv format version 2, where the primary key and the prefix lookup key are
     *       encoded with the compacted encoding (version 1 encodes them in the Iceberg encoding, so
     *       the prefix bytes would never be a prefix of the stored keys);
     *   <li>the bucket key is a strict, ordered prefix of the physical primary key, which is the
     *       only shape that enables a prefix lookup.
     * </ul>
     *
     * @param bucketKeys the bucket keys of the table
     * @param physicalPrimaryKeys the primary keys excluding partition keys, empty for log tables
     * @param kvFormatVersion the kv format version of the table, null for log tables and for legacy
     *     primary key tables (version 1)
     * @throws InvalidTableException if the bucket keys are not supported
     */
    public static void validateBucketKeys(
            List<String> bucketKeys,
            List<String> physicalPrimaryKeys,
            @Nullable Integer kvFormatVersion) {
        if (bucketKeys.size() <= 1) {
            return;
        }
        String reason;
        if (physicalPrimaryKeys.isEmpty()) {
            reason = "the table is a log table";
        } else if (kvFormatVersion == null || kvFormatVersion != KV_FORMAT_VERSION_2) {
            reason =
                    String.format(
                            "the table must use kv format version %s, but uses version %s",
                            KV_FORMAT_VERSION_2, kvFormatVersion == null ? 1 : kvFormatVersion);
        } else if (bucketKeys.size() >= physicalPrimaryKeys.size()
                || !physicalPrimaryKeys.subList(0, bucketKeys.size()).equals(bucketKeys)) {
            reason =
                    String.format(
                            "it must be a strict prefix of the primary key %s (excluding partition keys)",
                            physicalPrimaryKeys);
        } else {
            return;
        }
        throw new InvalidTableException(
                String.format(
                        "Iceberg tables only support a bucket key with multiple columns as a "
                                + "strict prefix of the primary key, but the bucket key %s is not "
                                + "supported: %s. Rows of an Iceberg table are distributed by "
                                + "the first bucket key column only, so use a single bucket key "
                                + "column '%s' instead, or a strict prefix of the primary key "
                                + "if prefix lookups need more columns.",
                        bucketKeys, reason, bucketKeys.get(0)));
    }
}
