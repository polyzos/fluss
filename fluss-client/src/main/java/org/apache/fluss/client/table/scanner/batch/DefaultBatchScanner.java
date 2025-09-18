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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.PartitionFilter;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FullScanRequest;
import org.apache.fluss.rpc.messages.FullScanResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Default implementation of {@link BatchScanner} that performs a full scan against tablet servers.
 *
 * <p>This scanner issues FULL_SCAN RPCs to the leaders of all buckets and aggregates the results.
 * It returns all current values at a point in time for primary-key tables. The first call to {@link
 * #pollBatch(Duration)} returns the complete snapshot; subsequent calls return {@code null}.
 *
 * <p>Note: For partitioned tables, callers may provide a {@link PartitionFilter} with a partition
 * name to restrict the scan to a single partition.
 */
public class DefaultBatchScanner implements BatchScanner {

    @Override
    public BatchScanner snapshotAll() {
        return this;
    }

    @Override
    public BatchScanner snapshotAllPartition(String partitionName) {
        if (!tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(
                    "Partition filter is only supported for partitioned tables");
        }
        return new DefaultBatchScanner(
                tableInfo,
                metadataUpdater,
                projectedFields,
                PartitionFilter.ofPartitionName(partitionName));
    }

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;
    @Nullable private final PartitionFilter partitionFilter;

    private final InternalRow.FieldGetter[] fieldGetters;
    private final ValueDecoder kvValueDecoder;

    private boolean endOfInput = false;

    public DefaultBatchScanner(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            @Nullable PartitionFilter partitionFilter) {
        this.tableInfo = Objects.requireNonNull(tableInfo, "tableInfo");
        this.metadataUpdater = Objects.requireNonNull(metadataUpdater, "metadataUpdater");
        this.projectedFields = projectedFields;
        this.partitionFilter = partitionFilter;

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        try {
            List<CompletableFuture<FullScanResponse>> futures = issueFullScanRequests();
            // wait for all responses or timeout for this poll
            CompletableFuture<Void> all =
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            all.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            List<InternalRow> rows = decodeFullScanResponses(futures);
            endOfInput = true;
            return CloseableIterator.wrap(rows.iterator());
        } catch (TimeoutException e) {
            // try again in next poll
            return CloseableIterator.emptyIterator();
        } catch (RuntimeException re) {
            // propagate runtime exceptions (e.g., IllegalArgumentException) without wrapping
            throw re;
        } catch (Exception e) {
            // wrap checked exceptions into IOException as the API declares
            throw new IOException(e);
        }
    }

    private List<CompletableFuture<FullScanResponse>> issueFullScanRequests() {
        Long partitionId = null;
        if (tableInfo.isPartitioned()) {
            if (partitionFilter == null || partitionFilter.getPartitionName() == null) {
                throw new IllegalArgumentException(
                        "Full scan on a partitioned table requires a PartitionFilter with a partition name.");
            }
            TablePath tablePath = tableInfo.getTablePath();
            PhysicalTablePath physicalTablePath =
                    PhysicalTablePath.of(tablePath, partitionFilter.getPartitionName());
            metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
            partitionId =
                    metadataUpdater
                            .getPartitionId(physicalTablePath)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Partition id not found for "
                                                            + partitionFilter.getPartitionName()));
        }

        long tableId = tableInfo.getTableId();
        int numBuckets = tableInfo.getNumBuckets();

        // collect leaders for all buckets
        HashSet<Integer> leaderServers = new HashSet<>();
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
            int leader = metadataUpdater.leaderFor(tableBucket);
            leaderServers.add(leader);
        }

        List<CompletableFuture<FullScanResponse>> responseFutures = new ArrayList<>();
        for (int leader : leaderServers) {
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway == null) {
                throw new LeaderNotAvailableException(
                        "Server " + leader + " is not found in metadata cache.");
            }
            FullScanRequest request = new FullScanRequest().setTableId(tableId);
            if (partitionId != null) {
                request.setPartitionId(partitionId);
            }
            // Future-proof: optionally pass bucket list when supported by server
            // request.setBucketId(Collections.singletonList(bucketId));
            responseFutures.add(gateway.fullScan(request));
        }
        return responseFutures;
    }

    private List<InternalRow> decodeFullScanResponses(
            List<CompletableFuture<FullScanResponse>> responseFutures) {
        List<InternalRow> out = new ArrayList<>();
        for (CompletableFuture<FullScanResponse> responseFuture : responseFutures) {
            FullScanResponse response = responseFuture.join();
            if (response.hasErrorCode() && response.getErrorCode() != Errors.NONE.code()) {
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
                    out.add(maybeProject(record.getRow()));
                }
            }
        }
        return out;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // deep copy and project if requested to avoid referencing released buffers
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
            projectedRow.replaceRow(newRow);
            return projectedRow;
        } else {
            return newRow;
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
