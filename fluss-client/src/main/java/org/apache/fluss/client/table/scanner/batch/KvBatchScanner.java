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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.messages.ScannerKeepAliveRequest;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.SchemaUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BatchScanner} implementation that streams KV records from a primary-key table bucket
 * using the server-side cursor-based {@code ScanKv} RPC.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>The scanner opens itself automatically on the first {@link #pollBatch} call.
 *   <li>Pipelining: immediately after a response is received, the next fetch request is sent to
 *       the server so that data transfer overlaps with the caller's processing.
 *   <li>The scanner closes automatically when the server signals {@code has_more_results = false}.
 *   <li>To abort early, call {@link #close()} explicitly.
 * </ol>
 *
 * <p>Not reusable: create a new scanner for each full scan.
 *
 * @since 0.9
 */
@PublicEvolving
@ThreadSafe
public class KvBatchScanner implements BatchScanner {

    private static final Logger LOG = LoggerFactory.getLogger(KvBatchScanner.class);

    private enum State {
        INIT,
        SCANNING,
        DONE,
        CLOSED
    }

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;
    @Nullable private final Integer limit;
    private final int batchSizeBytes;
    private final SchemaGetter schemaGetter;
    private final KvFormat kvFormat;
    private final int targetSchemaId;

    private final InternalRow.FieldGetter[] fieldGetters;

    /**
     * Schema-projection cache: sourceSchemaId → index mapping to target schema.
     * Guarded by the scanner lock; used in single-threaded poll context.
     */
    private final Map<Short, int[]> schemaProjectionCache = new HashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    @GuardedBy("lock")
    private State state = State.INIT;

    /** The gateway to the leader tablet server for this bucket. Set on first use. */
    @GuardedBy("lock")
    @Nullable
    private TabletServerGateway gateway;

    /** The scanner ID returned by the server after the initial request. */
    @GuardedBy("lock")
    @Nullable
    private byte[] scannerId;

    /** The next call_seq_id to send to the server. */
    @GuardedBy("lock")
    private int nextCallSeqId = 0;

    /**
     * In-flight future: either the initial request or the pre-fetched next-page request.
     * Pipelining: a new fetch is sent immediately after a response is received, before the
     * caller calls {@link #pollBatch} again.
     */
    @GuardedBy("lock")
    @Nullable
    private CompletableFuture<ScanKvResponse> inFlightFuture;

    /**
     * The log offset at the time the scanner was created on the server. Populated from the
     * initial ScanKvResponse and available for callers that need to switch to log scanning.
     */
    private volatile long logOffset = -1L;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Keep-alive support
    @Nullable private ScheduledExecutorService keepAliveExecutor;
    @Nullable private ScheduledFuture<?> keepAliveTask;

    public KvBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            SchemaGetter schemaGetter,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            @Nullable Integer limit,
            int batchSizeBytes) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataUpdater = metadataUpdater;
        this.projectedFields = projectedFields;
        this.limit = limit;
        this.batchSizeBytes = batchSizeBytes;
        this.schemaGetter = schemaGetter;
        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.targetSchemaId = tableInfo.getSchemaId();

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    /**
     * Returns the log offset captured on the server at the moment the scanner was created.
     * This is {@code -1} until the first successful response is received.
     *
     * <p>Flink streaming sources use this value to switch from KV scan to log scanning at
     * exactly the right offset.
     */
    public long getLogOffset() {
        return logOffset;
    }

    /**
     * Starts sending keep-alive requests to the server at the specified interval. This prevents
     * the server from expiring the scanner session while the caller is processing batches.
     *
     * <p>This method has no effect if the scanner has not been opened yet or is already closed.
     * It is safe to call this method multiple times (subsequent calls are ignored).
     *
     * @param keepAliveIntervalMs the interval in milliseconds between keep-alive requests
     */
    public synchronized void startKeepAlivePeriodically(int keepAliveIntervalMs) {
        if (keepAliveExecutor != null || closed.get()) {
            return;
        }
        keepAliveExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kv-scanner-keepalive");
            t.setDaemon(true);
            return t;
        });
        keepAliveTask = keepAliveExecutor.scheduleAtFixedRate(
                this::sendKeepAlive,
                keepAliveIntervalMs,
                keepAliveIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    private void sendKeepAlive() {
        lock.lock();
        byte[] currentScannerId;
        TabletServerGateway gw;
        try {
            if (state != State.SCANNING || scannerId == null || gateway == null) {
                return;
            }
            currentScannerId = scannerId;
            gw = gateway;
        } finally {
            lock.unlock();
        }

        try {
            ScannerKeepAliveRequest req = new ScannerKeepAliveRequest()
                    .setScannerId(currentScannerId);
            gw.scannerKeepAlive(req);
            LOG.trace("Sent keep-alive for scanner {}", new String(currentScannerId));
        } catch (Exception e) {
            LOG.warn("Failed to send keep-alive for scanner {}", new String(currentScannerId), e);
        }
    }

    /**
     * Polls for the next batch of records from the server.
     *
     * <p>On the first call, sends an initial {@code ScanKvRequest} to open the scanner on the
     * server. On subsequent calls, waits for the pre-fetched response from the previous call.
     *
     * @param timeout the maximum time to block waiting for a response
     * @return an iterator over the next batch of rows; an empty iterator if no data is ready
     *     within the timeout; or {@code null} when all rows have been consumed
     */
    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        lock.lock();
        try {
            if (state == State.CLOSED) {
                return null;
            }
            if (state == State.DONE) {
                return null;
            }
            if (state == State.INIT) {
                // First call: open the scanner and send the initial request
                openScanner();
                state = State.SCANNING;
            }

            // At this point, inFlightFuture holds the next fetch request
            if (inFlightFuture == null) {
                return null;
            }

            ScanKvResponse response;
            try {
                response = inFlightFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                return CloseableIterator.emptyIterator();
            } catch (Exception e) {
                throw new IOException("Failed to fetch KV scan page", e);
            }

            // Check for server-side error
            if (response.hasErrorCode()) {
                throw new IOException(
                        "Server returned error for KV scan: ["
                                + response.getErrorCode()
                                + "] "
                                + (response.hasErrorMessage() ? response.getErrorMessage() : ""));
            }

            // Capture log offset from initial response
            if (response.hasLogOffset()) {
                logOffset = response.getLogOffset();
            }

            // Update scanner ID from the response (initial response only)
            if (response.hasScannerId()) {
                scannerId = response.getScannerId();
            }

            boolean hasMore = response.hasHasMoreResults() && response.getHasMoreResults();

            // Pipeline: send next fetch immediately if there's more data
            if (hasMore && scannerId != null) {
                inFlightFuture = sendFetchRequest(scannerId, nextCallSeqId, false);
                nextCallSeqId++;
            } else {
                inFlightFuture = null;
                state = State.DONE;
                stopKeepAlive();
            }

            // Decode records
            if (!response.hasRecords()) {
                return hasMore ? CloseableIterator.emptyIterator() : null;
            }
            List<InternalRow> rows = decodeRecords(response.getRecords());
            return rows.isEmpty() && !hasMore
                    ? null
                    : CloseableIterator.wrap(rows.iterator());
        } finally {
            lock.unlock();
        }
    }

    /** Opens the scanner by sending the initial {@code ScanKvRequest} to the server. */
    @GuardedBy("lock")
    private void openScanner() {
        if (tableBucket.getPartitionId() != null) {
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        }
        int leaderId = metadataUpdater.leaderFor(tableInfo.getTablePath(), tableBucket);
        gateway = metadataUpdater.newTabletServerClientForNode(leaderId);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Tablet server " + leaderId + " is not available in metadata cache.");
        }

        PbScanReqForBucket bucketReq =
                new PbScanReqForBucket()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket());

        if (tableBucket.getPartitionId() != null) {
            bucketReq.setPartitionId(tableBucket.getPartitionId());
        }
        if (limit != null && limit > 0) {
            bucketReq.setLimit(limit);
        }

        ScanKvRequest initReq =
                new ScanKvRequest()
                        .setBucketScanReq(bucketReq)
                        .setCallSeqId(nextCallSeqId)
                        .setBatchSizeBytes(batchSizeBytes);
        nextCallSeqId++;

        inFlightFuture = gateway.scanKv(initReq);
    }

    /**
     * Sends a fetch request for an existing scanner session.
     */
    private CompletableFuture<ScanKvResponse> sendFetchRequest(
            byte[] currentScannerId, int seqId, boolean close) {
        ScanKvRequest req =
                new ScanKvRequest()
                        .setScannerId(currentScannerId)
                        .setCallSeqId(seqId)
                        .setBatchSizeBytes(batchSizeBytes);
        if (close) {
            req.setCloseScanner(true);
        }
        return gateway.scanKv(req);
    }

    /**
     * Decodes a {@code DefaultValueRecordBatch} byte array into a list of {@link InternalRow}s,
     * applying schema evolution projection and optional column pushdown.
     */
    private List<InternalRow> decodeRecords(byte[] recordsBytes) {
        List<InternalRow> rows = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(recordsBytes);
        DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToByteBuffer(buf);
        ValueRecordReadContext ctx =
                ValueRecordReadContext.createReadContext(schemaGetter, kvFormat);

        for (ValueRecord record : batch.records(ctx)) {
            InternalRow row = record.getRow();

            // Schema evolution: map old schema fields to the target schema
            if (targetSchemaId != record.schemaId()) {
                int[] indexMapping =
                        schemaProjectionCache.computeIfAbsent(
                                record.schemaId(),
                                srcId ->
                                        SchemaUtil.getIndexMapping(
                                                schemaGetter.getSchema(srcId),
                                                schemaGetter.getSchema(targetSchemaId)));
                row = ProjectedRow.from(indexMapping).replaceRow(row);
            }

            rows.add(materializeAndProject(row));
        }
        return rows;
    }

    /**
     * Deep-copies the row (to avoid holding references into transient network buffers) and applies
     * optional column projection.
     */
    private InternalRow materializeAndProject(InternalRow originRow) {
        // Deep copy to decouple from any underlying network buffer
        GenericRow copy = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            copy.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projected = ProjectedRow.from(projectedFields);
            projected.replaceRow(copy);
            return projected;
        }
        return copy;
    }

    private void stopKeepAlive() {
        if (keepAliveTask != null) {
            keepAliveTask.cancel(false);
            keepAliveTask = null;
        }
        if (keepAliveExecutor != null) {
            keepAliveExecutor.shutdownNow();
            keepAliveExecutor = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        lock.lock();
        try {
            stopKeepAlive();
            if (state == State.SCANNING && scannerId != null && gateway != null) {
                // Ask the server to close the scanner; fire-and-forget
                try {
                    ScanKvRequest closeReq =
                            new ScanKvRequest()
                                    .setScannerId(scannerId)
                                    .setCallSeqId(nextCallSeqId)
                                    .setBatchSizeBytes(0)
                                    .setCloseScanner(true);
                    gateway.scanKv(closeReq);
                } catch (Exception e) {
                    LOG.warn("Failed to send close request for scanner {}", new String(scannerId), e);
                }
            }
            if (inFlightFuture != null) {
                inFlightFuture.cancel(true);
                inFlightFuture = null;
            }
            state = State.CLOSED;
        } finally {
            lock.unlock();
        }
    }
}
