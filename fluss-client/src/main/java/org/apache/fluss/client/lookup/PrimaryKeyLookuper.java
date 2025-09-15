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

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Client-side lookuper implementation for primary-key tables.
 *
 * <p>This class supports single-key lookups as well as bounded, point-in-time snapshot reads that
 * return all current values of a KV table (and per-partition for partitioned tables). Snapshot
 * reads are executed via the FULL_SCAN RPC and aggregate results across the leaders of all buckets
 * on each target server.
 */
class PrimaryKeyLookuper implements Lookuper {

    private final TableInfo tableInfo;

    private final MetadataUpdater metadataUpdater;

    private final LookupClient lookupClient;

    private final KeyEncoder primaryKeyEncoder;

    /**
     * Extract bucket key from lookup key row, use {@link #primaryKeyEncoder} if is default bucket
     * key (bucket key = physical primary key).
     */
    private final KeyEncoder bucketKeyEncoder;

    private final BucketingFunction bucketingFunction;
    private final int numBuckets;

    /** a getter to extract partition from lookup key row, null when it's not a partitioned. */
    private @Nullable final PartitionGetter partitionGetter;

    /** Decode the lookup bytes to result row. */
    private final ValueDecoder kvValueDecoder;

    /**
     * Creates a primary-key lookuper for the given table.
     *
     * @param tableInfo resolved table metadata; must represent a table with a primary key
     * @param metadataUpdater client-side metadata cache/updater used to resolve leaders and
     *     partitions
     * @param lookupClient shared client used for key-based lookups and background I/O
     * @throws IllegalArgumentException if the table is not a primary-key table
     */
    public PrimaryKeyLookuper(
            TableInfo tableInfo, MetadataUpdater metadataUpdater, LookupClient lookupClient) {
        checkArgument(
                tableInfo.hasPrimaryKey(),
                "Log table %s doesn't support lookup",
                tableInfo.getTablePath());
        this.tableInfo = tableInfo;
        this.numBuckets = tableInfo.getNumBuckets();
        this.metadataUpdater = metadataUpdater;
        this.lookupClient = lookupClient;

        // the row type of the input lookup row
        RowType lookupRowType = tableInfo.getRowType().project(tableInfo.getPrimaryKeys());
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);

        // the encoded primary key is the physical primary key
        this.primaryKeyEncoder =
                KeyEncoder.of(lookupRowType, tableInfo.getPhysicalPrimaryKeys(), lakeFormat);
        this.bucketKeyEncoder =
                tableInfo.isDefaultBucketKey()
                        ? primaryKeyEncoder
                        : KeyEncoder.of(lookupRowType, tableInfo.getBucketKeys(), lakeFormat);
        this.bucketingFunction = BucketingFunction.of(lakeFormat);

        this.partitionGetter =
                tableInfo.isPartitioned()
                        ? new PartitionGetter(lookupRowType, tableInfo.getPartitionKeys())
                        : null;
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                tableInfo.getRowType().getChildren().toArray(new DataType[0])));
    }

    /**
     * Lookup a single row by its primary key.
     *
     * <p>The provided {@code lookupKey} must contain the primary-key fields in the expected order
     * for this table. If the table is partitioned, the partition may be derived from the key (if
     * configured) and routed accordingly.
     *
     * @param lookupKey the key row
     * @return a future with the lookup result; the row may be null if not found
     */
    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow lookupKey) {
        // encoding the key row using a compacted way consisted with how the key is encoded when put
        // a row
        byte[] pkBytes = primaryKeyEncoder.encodeKey(lookupKey);
        byte[] bkBytes =
                bucketKeyEncoder == primaryKeyEncoder
                        ? pkBytes
                        : bucketKeyEncoder.encodeKey(lookupKey);
        Long partitionId = null;
        if (partitionGetter != null) {
            try {
                partitionId =
                        getPartitionId(
                                lookupKey,
                                partitionGetter,
                                tableInfo.getTablePath(),
                                metadataUpdater);
            } catch (PartitionNotExistException e) {
                return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
            }
        }

        int bucketId = bucketingFunction.bucketing(bkBytes, numBuckets);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        return lookupClient
                .lookup(tableBucket, pkBytes)
                .thenApply(
                        valueBytes -> {
                            InternalRow row =
                                    valueBytes == null
                                            ? null
                                            : kvValueDecoder.decodeValue(valueBytes).row;
                            return new LookupResult(row);
                        });
    }

    /**
     * Returns all current values across all buckets for a non-partitioned primary-key table using a
     * point-in-time snapshot.
     *
     * @return a future with an unordered list of current rows
     */
    @Override
    public CompletableFuture<List<InternalRow>> snapshotAll() {
        if (tableInfo.isPartitioned()) {
            CompletableFuture<List<InternalRow>> result = new CompletableFuture<>();
            result.completeExceptionally(
                    new TableNotPartitionedException(
                            "Table is partitioned. Please use snapshotAllPartition(partitionName)."));

            return result;
        }

        return executeFullScan(Optional.empty());
    }

    /**
     * Returns all current values for the specified partition of a partitioned primary-key table
     * using a point-in-time snapshot.
     *
     * @param partitionName the partition identifier (for example, a date string)
     * @return a future with an unordered list of current rows in the partition
     */
    @Override
    public CompletableFuture<List<InternalRow>> snapshotAllPartition(String partitionName) {
        if (!tableInfo.isPartitioned()) {
            CompletableFuture<List<InternalRow>> result = new CompletableFuture<>();
            result.completeExceptionally(
                    new TableNotPartitionedException(
                            "Table is not partitioned. Please use snapshotAll()."));
            return result;
        }

        // Resolve partition id from name
        TablePath tablePath = tableInfo.getTablePath();
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);

        try {
            metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        } catch (PartitionNotExistException e) {
            CompletableFuture<List<InternalRow>> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }

        Optional<Long> partitionId = metadataUpdater.getPartitionId(physicalTablePath);
        return executeFullScan(partitionId);
    }

    /**
     * Decodes and aggregates FULL_SCAN RPC responses into a list of rows.
     *
     * <p>Throws a mapped client exception if any response contains an error.
     *
     * @param responseFutures futures of server responses per target leader node
     * @return aggregated, unordered rows
     */
    private List<InternalRow> decodeFullScanResponses(
            List<CompletableFuture<FullScanResponse>> responseFutures) {
        List<InternalRow> out = new ArrayList<>();
        for (CompletableFuture<FullScanResponse> responseFuture : responseFutures) {
            FullScanResponse response = responseFuture.join();

            if (response.hasErrorCode()
                    && response.getErrorCode()
                            != org.apache.fluss.rpc.protocol.Errors.NONE.code()) {
                Errors err = Errors.forCode(response.getErrorCode());
                throw err.exception(
                        response.hasErrorMessage() ? response.getErrorMessage() : err.message());
            }

            if (response.hasRecords()) {
                ByteBuffer buffer = ByteBuffer.wrap(response.getRecords());
                DefaultValueRecordBatch values = DefaultValueRecordBatch.pointToByteBuffer(buffer);

                ValueRecordReadContext context =
                        new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
                for (ValueRecord record : values.records(context)) {
                    out.add(record.getRow());
                }
            }
        }
        return out;
    }

    private CompletableFuture<List<InternalRow>> executeFullScan(Optional<Long> partitionIdOpt) {
        Long partitionId = partitionIdOpt.orElse(null);
        long tableId = tableInfo.getTableId();
        int numBuckets = tableInfo.getNumBuckets();

        // Find leader tablet/servers for this table/partition
        HashSet<Integer> leaderServers = new HashSet<>();
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

            // ensure metadata for this bucket is present
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
            int leader = metadataUpdater.leaderFor(tableBucket);
            leaderServers.add(leader);
        }

        List<CompletableFuture<FullScanResponse>> responseFutures = new ArrayList<>();
        for (int leader : leaderServers) {
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway == null) {
                CompletableFuture<List<InternalRow>> result = new CompletableFuture<>();
                result.completeExceptionally(
                        new LeaderNotAvailableException(
                                "Leader server " + leader + " is not found in metadata cache."));
                return result;
            }

            FullScanRequest request = new FullScanRequest();
            request.setTableId(tableId);

            if (partitionId != null) {
                request.setPartitionId(partitionId);
            }
            responseFutures.add(gateway.fullScan(request));
        }
        // Decode all responses and aggregate rows
        return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> decodeFullScanResponses(responseFutures));
    }
}
