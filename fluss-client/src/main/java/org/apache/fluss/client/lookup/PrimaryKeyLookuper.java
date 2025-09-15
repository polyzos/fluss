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
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.client.utils.ClientUtils.getPartitionId;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** An implementation of {@link Lookuper} that lookups by primary key. */
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

    @Override
    public CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> snapshotAll() {
        // Non-partitioned table only. For partitioned tables, prefer snapshotAllPartition.
        if (tableInfo.isPartitioned()) {
            CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> f = new CompletableFuture<>();
            f.completeExceptionally(new org.apache.fluss.exception.TableNotPartitionedException(
                    "Table is partitioned. Please call snapshotAllPartition(partitionName)."));
            return f;
        }
        return sendFullScan(null);
    }

    @Override
    public CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> snapshotAllPartition(String partitionName) {
        if (!tableInfo.isPartitioned()) {
            CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> f = new CompletableFuture<>();
            f.completeExceptionally(new org.apache.fluss.exception.TableNotPartitionedException(
                    "Table is not partitioned."));
            return f;
        }
        // Resolve partition id from name
        org.apache.fluss.metadata.TablePath tablePath = tableInfo.getTablePath();
        org.apache.fluss.metadata.PhysicalTablePath physical = org.apache.fluss.metadata.PhysicalTablePath.of(tablePath, partitionName);
        try {
            metadataUpdater.checkAndUpdatePartitionMetadata(physical);
        } catch (org.apache.fluss.exception.PartitionNotExistException e) {
            CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }
        java.util.Optional<Long> pidOpt = metadataUpdater.getPartitionId(physical);
        if (!pidOpt.isPresent()) {
            CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> f = new CompletableFuture<>();
            f.completeExceptionally(new org.apache.fluss.exception.PartitionNotExistException(
                    "Partition '" + partitionName + "' not found for table " + tablePath));
            return f;
        }
        return sendFullScan(pidOpt.get());
    }

    private CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> sendFullScan(@Nullable Long partitionId) {
        long tableId = tableInfo.getTableId();
        int buckets = tableInfo.getNumBuckets();
        // Compute unique leader tablet servers for this table/partition
        java.util.Set<Integer> leaderServers = new java.util.HashSet<>();
        for (int b = 0; b < buckets; b++) {
            TableBucket tb = new TableBucket(tableId, partitionId, b);
            // Ensure metadata present for this bucket
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tb);
            int leader = metadataUpdater.leaderFor(tb);
            leaderServers.add(leader);
        }
        java.util.List<java.util.concurrent.CompletableFuture<org.apache.fluss.rpc.messages.FullScanResponse>> futures = new java.util.ArrayList<>();
        for (Integer serverId : leaderServers) {
            org.apache.fluss.rpc.gateway.TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
            if (gateway == null) {
                java.util.concurrent.CompletableFuture<java.util.List<org.apache.fluss.row.InternalRow>> f = new java.util.concurrent.CompletableFuture<>();
                f.completeExceptionally(new org.apache.fluss.exception.LeaderNotAvailableException("Server " + serverId + " is not found in metadata cache."));
                return f;
            }
            org.apache.fluss.rpc.messages.FullScanRequest req = new org.apache.fluss.rpc.messages.FullScanRequest().setTableId(tableId);
            if (partitionId != null) {
                req.setPartitionId(partitionId);
            }
            futures.add(gateway.fullScan(req));
        }
        // Decode all responses and aggregate rows
        return java.util.concurrent.CompletableFuture
                .allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                .thenApply(v -> {
                    java.util.List<org.apache.fluss.row.InternalRow> out = new java.util.ArrayList<>();
                    for (java.util.concurrent.CompletableFuture<org.apache.fluss.rpc.messages.FullScanResponse> f : futures) {
                        org.apache.fluss.rpc.messages.FullScanResponse resp = f.join();
                        if (resp.hasErrorCode() && resp.getErrorCode() != org.apache.fluss.rpc.protocol.Errors.NONE.code()) {
                            org.apache.fluss.rpc.protocol.Errors err = org.apache.fluss.rpc.protocol.Errors.forCode(resp.getErrorCode());
                            throw err.exception(resp.hasErrorMessage() ? resp.getErrorMessage() : err.message());
                        }
                        if (resp.hasRecords()) {
                            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(resp.getRecords());
                            org.apache.fluss.record.DefaultValueRecordBatch values = org.apache.fluss.record.DefaultValueRecordBatch.pointToByteBuffer(buf);
                            org.apache.fluss.record.ValueRecordReadContext ctx = new org.apache.fluss.record.ValueRecordReadContext(kvValueDecoder.getRowDecoder());
                            for (org.apache.fluss.record.ValueRecord rec : values.records(ctx)) {
                                out.add(rec.getRow());
                            }
                        }
                    }
                    return out;
                });
    }
}
