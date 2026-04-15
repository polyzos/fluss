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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link BatchScanner} that streams all live rows from a single KV bucket by iterating the tablet
 * server's RocksDB instance via a sequence of ScanKv RPCs.
 *
 * <p>The remote scanner is opened lazily on the first {@link #pollBatch} call. Once the bucket is
 * exhausted the scanner closes itself. If the caller needs to abort early it must call {@link
 * #close} explicitly.
 *
 * <p>After each response with {@code has_more_results = true} the next RPC is fired immediately,
 * overlapping network latency with the caller's row processing. At most one request is in-flight at
 * any time.
 *
 * <p><b>Scanner TTL:</b> the server-side session expires after a configured period of inactivity
 * (default 60 s, controlled by {@code kv.scanner.ttl}). Because this scanner pipelines requests,
 * callers that process rows promptly will not encounter this limit. However, if the caller does
 * expensive work between calls and the elapsed time between two {@link #pollBatch} invocations
 * exceeds the server TTL, the next call will throw an {@link IOException} whose cause is a {@code
 * ScannerExpiredException}. In that case the scan cannot be resumed and a new scanner must be
 * opened.
 *
 * <p><b>Note on {@code log_offset}:</b> the first {@code ScanKvResponse} carries a {@code
 * log_offset} field that reflects the log high-watermark at the time the RocksDB snapshot was
 * opened. This field is not currently surfaced through the {@link BatchScanner} API and is
 * discarded.
 *
 * <p>Not reusable and not thread-safe.
 */
@Internal
public class KvBatchScanner implements BatchScanner {
    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);

    /** Batch size used when no explicit value is provided (matches the config default of 4 MiB). */
    static final int DEFAULT_BATCH_SIZE_BYTES = 4 * 1024 * 1024;

    /**
     * Maximum number of times the initial open RPC is retried when the server returns {@code
     * TOO_MANY_SCANNERS}. Retry delays follow an exponential back-off starting at {@link
     * #BASE_RETRY_DELAY_MS}: 100 ms, 200 ms, 400 ms.
     */
    static final int MAX_OPEN_RETRIES = 3;

    /** Base delay for the {@code TOO_MANY_SCANNERS} exponential back-off in milliseconds. */
    static final long BASE_RETRY_DELAY_MS = 100L;

    private final int batchSizeBytes;

    private final TablePath tablePath;
    private final TableBucket bucket;
    private final SchemaGetter schemaGetter;
    private final MetadataUpdater metadataUpdater;
    private final int targetSchemaId;

    /** User-requested column projection applied after schema evolution, or {@code null}. */
    @Nullable private final int[] projectedColumns;

    /** Cache for schema evolution index mappings. */
    private final Map<Short, int[]> schemaMappingCache = new HashMap<>();

    /** Reused across all batches; schemaGetter and kvFormat never change. */
    private final ValueRecordReadContext readContext;

    private boolean done = false;

    @Nullable private TabletServerGateway gateway;
    @Nullable private byte[] scannerId;

    /** Monotonically increasing ID sent with each continuation request. */
    private int callSeqId = 0;

    /**
     * Number of times the initial open has been retried due to {@code TOO_MANY_SCANNERS}. Capped at
     * {@link #MAX_OPEN_RETRIES}.
     */
    private int openRetries = 0;

    @Nullable private CompletableFuture<ScanKvResponse> prefetchFuture;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater) {
        this(tableInfo, tableBucket, schemaGetter, metadataUpdater, DEFAULT_BATCH_SIZE_BYTES, null);
    }

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            int batchSizeBytes,
            @Nullable int[] projectedColumns) {
        this.tablePath = tableInfo.getTablePath();
        this.bucket = tableBucket;
        this.schemaGetter = schemaGetter;
        this.metadataUpdater = metadataUpdater;
        this.targetSchemaId = tableInfo.getSchemaId();
        this.batchSizeBytes = Math.max(1, batchSizeBytes);
        this.projectedColumns = projectedColumns;
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
     *   <li>Returns {@code null} when the bucket is exhausted or the scanner has been closed.
     * </ul>
     *
     * <p>When opening the scanner fails with {@code TOO_MANY_SCANNERS}, this method automatically
     * retries up to {@link #MAX_OPEN_RETRIES} times with exponential back-off (100 ms, 200 ms, 400
     * ms), returning an empty iterator on each retry attempt. If the limit is still exceeded after
     * all retries, an {@link IOException} whose cause is {@code TooManyScannersException} is thrown
     * and the scanner is closed.
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
                throw new IOException("Failed to open scanner for bucket " + bucket, e);
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
            // TOO_MANY_SCANNERS only affects the initial open (scannerId not yet set).
            // Retry with exponential back-off up to MAX_OPEN_RETRIES times before giving up.
            if (response.getErrorCode() == Errors.TOO_MANY_SCANNERS.code()
                    && scannerId == null
                    && openRetries < MAX_OPEN_RETRIES) {
                long delayMs = BASE_RETRY_DELAY_MS * (1L << openRetries);
                openRetries++;
                LOG.debug(
                        "Too many scanners for bucket {}; retrying open in {} ms "
                                + "(attempt {}/{})",
                        bucket,
                        delayMs,
                        openRetries,
                        MAX_OPEN_RETRIES);
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    done = true;
                    throw new IOException(
                            "Interrupted while waiting to retry scanner open for bucket " + bucket,
                            ie);
                }
                openScanner();
                return CloseableIterator.emptyIterator();
            }
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
            done = true;
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
            gateway.scanKv(
                            new ScanKvRequest()
                                    .setScannerId(scannerId)
                                    .setBatchSizeBytes(batchSizeBytes)
                                    .setCloseScanner(true))
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

    private void openScanner() {
        metadataUpdater.checkAndUpdateMetadata(tablePath, bucket);
        int leader = metadataUpdater.leaderFor(tablePath, bucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Leader for bucket " + bucket + " is not available. Please retry the scan.");
        }

        PbScanReqForBucket bucketReq =
                new PbScanReqForBucket()
                        .setTableId(bucket.getTableId())
                        .setBucketId(bucket.getBucket());
        if (bucket.getPartitionId() != null) {
            bucketReq.setPartitionId(bucket.getPartitionId());
        }

        ScanKvRequest request =
                new ScanKvRequest().setBucketScanReq(bucketReq).setBatchSizeBytes(batchSizeBytes);
        prefetchFuture = gateway.scanKv(request);
    }

    private void sendContinuation() {
        ScanKvRequest request =
                new ScanKvRequest()
                        .setScannerId(scannerId)
                        .setBatchSizeBytes(batchSizeBytes)
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
            if (projectedColumns != null) {
                row = ProjectedRow.from(projectedColumns).replaceRow(row);
            }
            rows.add(row);
        }
        return rows;
    }
}
