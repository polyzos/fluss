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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FullScanRequest;
import org.apache.fluss.rpc.messages.FullScanResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * BatchScanner that performs a FULL_SCAN snapshot across all leaders of a KV table (or a single
 * partition) and exposes the result via pollBatch in a bounded fashion.
 */
@Internal
public class FullScanBatchScanner implements BatchScanner {

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final Long partitionId;

    private final ValueDecoder kvValueDecoder;

    private final CompletableFuture<List<InternalRow>> rowsFuture;

    private volatile boolean emitted = false;

    public FullScanBatchScanner(
            TableInfo tableInfo, MetadataUpdater metadataUpdater, @Nullable Long partitionId) {
        this.tableInfo = Objects.requireNonNull(tableInfo, "tableInfo");
        this.metadataUpdater = Objects.requireNonNull(metadataUpdater, "metadataUpdater");
        this.partitionId = partitionId;

        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                tableInfo.getRowType().getChildren().toArray(new DataType[0])));

        this.rowsFuture = new CompletableFuture<>();
        // start async scan
        CompletableFuture.runAsync(this::startFullScan);
    }

    private void startFullScan() {
        try {
            long tableId = tableInfo.getTableId();
            int numBuckets = tableInfo.getNumBuckets();

            // Send full scan request per bucket to its leader tablet/server
            List<CompletableFuture<FullScanResponse>> responseFutures = new ArrayList<>();
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
                int leader = metadataUpdater.leaderFor(tableBucket);

                TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
                if (gateway == null) {
                    rowsFuture.completeExceptionally(
                            new IllegalStateException(
                                    "Leader server "
                                            + leader
                                            + " is not found in metadata cache."));
                    return;
                }
                FullScanRequest request = new FullScanRequest();
                request.setTableId(tableId);
                request.setBucketId(bucketId);
                if (partitionId != null) {
                    request.setPartitionId(partitionId);
                }
                responseFutures.add(gateway.fullScan(request));
            }

            CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> decodeFullScanResponses(responseFutures))
                    .whenComplete(
                            (rows, err) -> {
                                if (err != null) {
                                    rowsFuture.completeExceptionally(err);
                                } else {
                                    rowsFuture.complete(rows);
                                }
                            });
        } catch (Throwable t) {
            rowsFuture.completeExceptionally(t);
        }
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
                    out.add(record.getRow());
                }
            }
        }
        return out;
    }

    @Override
    public @Nullable CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (emitted) {
            return null;
        }
        try {
            List<InternalRow> rows;
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                rows = rowsFuture.get();
            } else {
                rows = rowsFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            emitted = true;
            return CloseableIterator.wrap(rows.iterator());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IOException("Failed to perform full scan", cause);
        } catch (Exception te) {
            return CloseableIterator.emptyIterator();
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    @Override
    public CompletableFuture<List<InternalRow>> snapshotAll() {
        return rowsFuture;
    }

    @Override
    public CompletableFuture<List<InternalRow>> snapshotAllPartition(String partitionName) {
        throw new UnsupportedOperationException(
                "This scanner is already bound to a specific scope; use TableScan#createBatchScanner(partitionName) then snapshotAll().");
    }
}
