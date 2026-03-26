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
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link BatchScanner} that streams all live rows from one or more KV buckets by iterating each
 * tablet server's RocksDB instance via a sequence of ScanKv RPCs.
 *
 * <p>Buckets are scanned sequentially. When one bucket is fully consumed the scanner automatically
 * advances to the next. The remote scanner for each bucket is opened lazily on the first {@link
 * #pollBatch} call for that bucket. If the caller needs to abort early it must call {@link #close}
 * explicitly.
 *
 * <p>After each response with {@code has_more_results = true} the next RPC is fired immediately,
 * overlapping network latency with the caller's row processing. At most one request is in-flight at
 * any time.
 *
 * <p>Not reusable and not thread-safe.
 */
public class KvBatchScanner implements BatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);
    private static final int BATCH_SIZE_BYTES = 4 * 1024 * 1024;

    private final TablePath tablePath;
    private final List<TableBucket> buckets;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final int targetSchemaId;

    /** Cache for schema evolution index mappings. */
    private final Map<Short, int[]> schemaMappingCache = new HashMap<>();

    /** Reused across all batches and buckets; schemaGetter and kvFormat never change. */
    private final ValueRecordReadContext readContext;

    private int currentBucketIndex = 0;
    private boolean done = false;

    // Per-bucket mutable state — reset each time the scanner advances to a new bucket.
    @Nullable private TabletServerGateway gateway;
    @Nullable private byte[] scannerId;

    /** Monotonically increasing ID sent with each continuation request, reset per bucket. */
    private int callSeqId = 0;

    @Nullable private CompletableFuture<ScanKvResponse> prefetchFuture;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater) {
        this(tableInfo, Collections.singletonList(tableBucket), schemaGetter, metadataUpdater);
    }

    public KvBatchScanner(
            TableInfo tableInfo,
            List<TableBucket> buckets,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater) {
        this.tablePath = tableInfo.getTablePath();
        this.buckets = buckets;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.readContext =
                ValueRecordReadContext.createReadContext(
                        schemaGetter, tableInfo.getTableConfig().getKvFormat());
    }

    /**
     * Returns the next batch of rows.
     *
     * <ul>
     *   <li>Returns an empty iterator if the in-flight RPC has not completed within {@code
     *       timeout}.
     *   <li>Returns {@code null} when all buckets are exhausted or the scanner has been closed.
     * </ul>
     */
    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (done) {
            return null;
        }
        if (prefetchFuture == null) {
            try {
                openScanner();
            } catch (Exception e) {
                done = true;
                // TODO: handle LeaderNotAvailableException with retry (see LimitBatchScanner).
                throw new IOException(
                        "Failed to open scanner for bucket " + currentBucket(), e);
            }
        }

        ScanKvResponse response;
        try {
            response = prefetchFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
        prefetchFuture = null;

        if (response.hasErrorCode() && response.getErrorCode() != 0) {
            done = true;
            throw new IOException(
                    Errors.forCode(response.getErrorCode())
                            .exception(
                                    response.hasErrorMessage()
                                            ? response.getErrorMessage()
                                            : null));
        }

        if (response.hasScannerId()) {
            scannerId = response.getScannerId();
        }

        boolean hasMore = response.hasHasMoreResults() && response.isHasMoreResults();
        if (hasMore) {
            sendContinuation();
        } else {
            advanceBucket();
        }

        if (!response.hasRecords()) {
            // Empty last batch or empty bucket.
            return done ? null : CloseableIterator.emptyIterator();
        }
        List<InternalRow> rows = parseRecords(response);
        return CloseableIterator.wrap(rows.iterator());
    }

    /**
     * Releases all resources. Cancels any in-flight prefetch and sends a {@code close_scanner=true}
     * RPC to free the server-side session immediately. Idempotent.
     */
    @Override
    public void close() throws IOException {
        if (done) {
            return;
        }
        done = true;
        if (prefetchFuture != null) {
            prefetchFuture.cancel(true);
            prefetchFuture = null;
        }
        if (scannerId != null && gateway != null) {
            gateway.scanKv(new ScanKvRequest().setScannerId(scannerId).setCloseScanner(true))
                    .whenComplete(
                            (resp, ex) -> {
                                if (ex != null) {
                                    LOG.debug(
                                            "close_scanner RPC failed for scanner {},"
                                                    + " server-side TTL cleanup will"
                                                    + " reclaim resources.",
                                            scannerId,
                                            ex);
                                }
                            });
        }
    }

    private TableBucket currentBucket() {
        return buckets.get(currentBucketIndex);
    }

    /**
     * Resets per-bucket state and advances to the next bucket. Sets {@link #done} when all buckets
     * have been consumed. The server already closed the scanner when {@code hasMore = false}, so no
     * explicit close RPC is needed here.
     */
    private void advanceBucket() {
        gateway = null;
        scannerId = null;
        callSeqId = 0;
        currentBucketIndex++;
        if (currentBucketIndex >= buckets.size()) {
            done = true;
        }
    }

    private void openScanner() {
        TableBucket bucket = currentBucket();
        if (bucket.getPartitionId() != null) {
            metadataUpdater.checkAndUpdateMetadata(tablePath, bucket);
        }
        int leader = metadataUpdater.leaderFor(tablePath, bucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }

        PbScanReqForBucket bucketReq =
                new PbScanReqForBucket()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }

        ScanKvRequest request =
                new ScanKvRequest().setBucketScanReq(bucketReq).setBatchSizeBytes(BATCH_SIZE_BYTES);
        prefetchFuture = gateway.scanKv(request);
    }

    private void sendContinuation() {
        ScanKvRequest request =
                new ScanKvRequest()
                        .setScannerId(scannerId)
                        .setBatchSizeBytes(BATCH_SIZE_BYTES)
                        .setCallSeqId(callSeqId++);
        prefetchFuture = gateway.scanKv(request);
    }

    private List<InternalRow> parseRecords(ScanKvResponse response) {
        List<InternalRow> rows = new ArrayList<>();
        ByteBuffer recordsBuffer = ByteBuffer.wrap(response.getRecords());
        DefaultValueRecordBatch valueRecords =
                DefaultValueRecordBatch.pointToByteBuffer(recordsBuffer);
        for (ValueRecord record : valueRecords.records(readContext)) {
            InternalRow row = record.getRow();
            if (targetSchemaId != record.schemaId()) {
                int[] indexMapping =
                        schemaMappingCache.computeIfAbsent(
                                record.schemaId(),
                                sourceSchemaId ->
                                        SchemaUtil.getIndexMapping(
                                                schemaGetter.getSchema(sourceSchemaId),
                                                schemaGetter.getSchema(targetSchemaId)));
                row = ProjectedRow.from(indexMapping).replaceRow(row);
            }
            rows.add(row);
        }
        return rows;
    }
}
