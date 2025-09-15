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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The lookuper is used to fetch rows from a table either by primary key or by a key prefix,
 * depending on how it is created.
 *
 * <p>Instances are created via {@link Lookup} builders (e.g., {@code table.newLookup()}) and can
 * target primary-key lookups (exact match) or prefix-key lookups. Some operations are only
 * supported for primary-key tables and will throw {@link UnsupportedOperationException} otherwise.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Lookuper {

    /**
     * Performs a lookup using the provided key.
     *
     * <p>The lookup key must be:
     *
     * <ul>
     *   <li>the primary key if this lookuper is a Primary Key Lookuper (created via {@code
     *       table.newLookup().createLookuper()}), or
     *   <li>a key prefix if this lookuper is a Prefix Key Lookuper (created via {@code
     *       table.newLookup().lookupBy(prefixKeys).createLookuper()}).
     * </ul>
     *
     * @param lookupKey the lookup key row (schema must match the expected key shape)
     * @return a future that completes with a {@link LookupResult} containing the matched row or
     *     empty if not found; the future may complete exceptionally on RPC or server errors
     */
    CompletableFuture<LookupResult> lookup(InternalRow lookupKey);

    /**
     * Returns all current values of a primary-key KV table using a point-in-time snapshot. This is
     * not a streaming/cursor operation; it materializes the full result as a list.
     *
     * <p>For partitioned tables, prefer {@link #snapshotAllPartition(String)}.
     *
     * @return a future completing with an unordered list of {@link InternalRow} representing the
     *     current values; the future may complete exceptionally if unsupported by the lookuper or
     *     on RPC/server errors
     * @throws UnsupportedOperationException if the implementation does not support snapshot reads
     */
    default CompletableFuture<List<InternalRow>> snapshotAll() {
        throw new UnsupportedOperationException(
                "snapshotAll() is only supported for primary key lookuper.");
    }

    /**
     * Returns all current values for the specified partition of a partitioned primary-key table,
     * using a point-in-time snapshot.
     *
     * @param partitionName the partition identifier (e.g., a date-based partition value)
     * @return a future completing with an unordered list of {@link InternalRow} for that partition;
     *     the future may complete exceptionally if unsupported by the lookuper or on RPC/server
     *     errors
     * @throws UnsupportedOperationException if the implementation does not support partition
     *     snapshot reads
     */
    default CompletableFuture<List<InternalRow>> snapshotAllPartition(String partitionName) {
        throw new UnsupportedOperationException(
                "snapshotAllPartition() is not supported by this lookuper.");
    }
}
