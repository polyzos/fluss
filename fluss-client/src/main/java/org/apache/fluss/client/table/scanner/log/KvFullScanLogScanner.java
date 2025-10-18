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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NewScanRequestPB;
import org.apache.fluss.rpc.messages.ScanRequest;
import org.apache.fluss.rpc.messages.ScanResponse;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.fluss.rpc.messages.ScannerKeepAliveRequest;
import org.apache.fluss.rpc.messages.ScannerKeepAliveResponse;

/**
 * A LogScanner implementation that streams the whole KV RocksDB from a tablet bucket via the
 * scanner-based kvScan RPC. It incrementally fetches value batches and exposes them as ScanRecords.
 */
@PublicEvolving
public class KvFullScanLogScanner implements LogScanner {

    private static final int DEFAULT_BATCH_SIZE_BYTES = 1 << 20; // 1 MiB per RPC batch
    private static final long KEEPALIVE_MIN_INTERVAL_MS = 10_000L; // 10s min interval

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;

    private volatile boolean closed;
    private TableBucket tableBucket;

    // rpc state
    private TabletServerGateway gateway;
    private byte[] scannerId;
    private int callSeqId;
    private byte[] queryId;
    private boolean hasMoreResults;
    private long lastKeepAliveMs;

    // decode state
    private final ValueDecoder kvValueDecoder;
    private final InternalRow.FieldGetter[] fieldGetters;

    public KvFullScanLogScanner(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields) {
        this.tableInfo = Objects.requireNonNull(tableInfo, "tableInfo");
        this.metadataUpdater = Objects.requireNonNull(metadataUpdater, "metadataUpdater");
        this.projectedFields = projectedFields;
        this.closed = false;
        this.gateway = null;
        this.scannerId = null;
        this.callSeqId = 0;
        this.queryId = null;
        this.hasMoreResults = true; // assume more until server says otherwise
        this.lastKeepAliveMs = 0L;

        RowType rowType = tableInfo.getRowType();
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    @Override
    public synchronized ScanRecords poll(Duration timeout) {
        if (closed) {
            throw new IllegalStateException("Scanner already closed.");
        }
        if (tableBucket == null) {
            throw new IllegalStateException("KvFullScanLogScanner is not subscribed any buckets.");
        }
        if (!hasMoreResults) {
            return ScanRecords.EMPTY;
        }

        ensureGateway();

        try {
            ScanRequest req = new ScanRequest();
            if (scannerId == null) {
                // first call: create scanner
                NewScanRequestPB newReq = new NewScanRequestPB().setBucketId(tableBucket.getBucket());
                if (tableBucket.getPartitionId() != null) {
                    newReq.setPartitionId(tableBucket.getPartitionId());
                    metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
                }
                req.setNewScanRequest(newReq);
                req.setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES);
                this.queryId = UUID.randomUUID().toString().getBytes();
                req.setQueryId(this.queryId);
                req.setCallSeqId(callSeqId);
            } else {
                req.setScannerId(scannerId).setCallSeqId(++callSeqId);
            }

            CompletableFuture<ScanResponse> future = gateway.scan(req);
            ScanResponse resp = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (resp.hasScannerId() && scannerId == null) {
                this.scannerId = resp.getScannerId();
                this.lastKeepAliveMs = System.currentTimeMillis();
            }
            this.hasMoreResults = resp.hasHasMoreResults() && resp.isHasMoreResults();

            List<ScanRecord> records = new ArrayList<>();
            if (resp.hasRecords()) {
                ByteBuffer buf = resp.getRecordsSlice().nioBuffer();
                DefaultValueRecordBatch valueBatch = DefaultValueRecordBatch.pointToByteBuffer(buf);
                ValueRecordReadContext ctx = new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
                for (ValueRecord vr : valueBatch.records(ctx)) {
                    records.add(new ScanRecord(maybeProject(vr.getRow())));
                }
            }

            if (records.isEmpty()) {
                return ScanRecords.EMPTY;
            }
            Map<TableBucket, List<ScanRecord>> map = new HashMap<>();
            map.put(tableBucket, records);
            return new ScanRecords(map);
        } catch (TimeoutException te) {
            // no data within timeout, caller can poll again
            maybeSendKeepAlive();
            return ScanRecords.EMPTY;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void maybeSendKeepAlive() {
        if (gateway == null || scannerId == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastKeepAliveMs < KEEPALIVE_MIN_INTERVAL_MS) {
            return;
        }
        lastKeepAliveMs = now;
        try {
            CompletableFuture<ScannerKeepAliveResponse> f =
                    gateway.scannerKeepAlive(new ScannerKeepAliveRequest().setScannerId(scannerId));
            // best-effort: don't block the caller; log or ignore exceptions
            f.exceptionally(ignore -> null);
        } catch (Throwable ignore) {
            // ignore keepalive failures
        }
    }

    private void ensureGateway() {
        if (gateway != null) {
            return;
        }
        int leader = metadataUpdater.leaderFor(tableBucket);
        TabletServerGateway g = metadataUpdater.newTabletServerClientForNode(leader);
        if (g == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }
        this.gateway = g;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projected = ProjectedRow.from(projectedFields);
            projected.replaceRow(newRow);
            return projected;
        }
        return newRow;
    }

    @Override
    public synchronized void subscribe(int bucket, long offset) {
        this.tableBucket = new TableBucket(tableInfo.getTableId(), bucket);
        resetState();
    }

    @Override
    public synchronized void subscribe(long partitionId, int bucket, long offset) {
        this.tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucket);
        metadataUpdater.checkAndUpdatePartitionMetadata(
                tableInfo.getTablePath(), java.util.Collections.singleton(partitionId));
        resetState();
    }

    private void resetState() {
        this.gateway = null;
        this.scannerId = null;
        this.callSeqId = 0;
        this.queryId = null;
        this.hasMoreResults = true;
        this.lastKeepAliveMs = 0L;
    }

    @Override
    public synchronized void unsubscribe(long partitionId, int bucket) {
        if (tableBucket != null
                && java.util.Objects.equals(tableBucket.getPartitionId(), partitionId)
                && tableBucket.getBucket() == bucket) {
            tableBucket = null;
        }
    }

    @Override
    public void wakeup() {
        // no-op for now
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        // best-effort close on server side
        if (gateway != null && scannerId != null) {
            try {
                gateway.scan(new ScanRequest().setScannerId(scannerId).setCloseScanner(true));
            } catch (Throwable ignore) {
                // ignore
            }
        }
    }
}
