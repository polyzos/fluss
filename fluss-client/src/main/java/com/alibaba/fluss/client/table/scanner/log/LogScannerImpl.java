/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.metrics.ScannerMetricGroup;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.client.table.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.WakeupException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The default impl of {@link LogScanner}.
 *
 * <p>The {@link LogScannerImpl} is NOT thread-safe. It is the responsibility of the user to ensure
 * that multithreaded access is properly synchronized. Un-synchronized access will result in {@link
 * ConcurrentModificationException}.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScannerImpl<T> implements LogScanner<T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogScannerImpl.class);

    private static final long NO_CURRENT_THREAD = -1L;
    private final TablePath tablePath;
    private final LogScannerStatus logScannerStatus;
    private final MetadataUpdater metadataUpdater;
    private final LogFetcher logFetcher;
    private final long tableId;
    private final boolean isPartitionedTable;

    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing FlussLogScanner
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread who has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);
    // metrics
    private final ScannerMetricGroup scannerMetricGroup;

    public LogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            RpcClient rpcClient,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields) {
        this.tablePath = tableInfo.getTablePath();
        this.tableId = tableInfo.getTableId();
        this.isPartitionedTable = tableInfo.isPartitioned();
        // add this table to metadata updater.
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        this.logScannerStatus = new LogScannerStatus();
        this.metadataUpdater = metadataUpdater;
        Projection projection = sanityProjection(projectedFields, tableInfo);
        this.scannerMetricGroup = new ScannerMetricGroup(clientMetricGroup, tablePath);
        this.logFetcher =
                new LogFetcher(
                        tableInfo,
                        projection,
                        rpcClient,
                        logScannerStatus,
                        conf,
                        metadataUpdater,
                        scannerMetricGroup,
                        remoteFileDownloader);
    }

    /**
     * Check if the projected fields are valid and returns {@link Projection} if the projection is
     * not null.
     */
    @Nullable
    private Projection sanityProjection(@Nullable int[] projectedFields, TableInfo tableInfo) {
        RowType tableRowType = tableInfo.getRowType();
        if (projectedFields != null) {
            for (int projectedField : projectedFields) {
                if (projectedField < 0 || projectedField >= tableRowType.getFieldCount()) {
                    throw new IllegalArgumentException(
                            "Projected field index "
                                    + projectedField
                                    + " is out of bound for schema "
                                    + tableRowType);
                }
            }
            return Projection.of(projectedFields);
        } else {
            return null;
        }
    }

    @Override
    public ScanRecords<T> poll(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException("LogScanner is not subscribed any buckets.");
            }

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            long timeoutNanos = timeout.toNanos();
            long startNanos = System.nanoTime();
            do {
                Map<TableBucket, List<ScanRecord<T>>> fetchRecords = pollForFetches();
                if (fetchRecords.isEmpty()) {
                    try {
                        if (!logFetcher.awaitNotEmpty(startNanos + timeoutNanos)) {
                            // logFetcher waits for the timeout and no data in buffer,
                            // so we return empty
                            return new ScanRecords<>(fetchRecords);
                        }
                    } catch (WakeupException e) {
                        // wakeup() is called, we need to return empty
                        return new ScanRecords<>(fetchRecords);
                    }
                } else {
                    // before returning the fetched records, we can send off the next round of
                    // fetches and avoid block waiting for their responses to enable pipelining
                    // while the user is handling the fetched records.
                    logFetcher.sendFetches();

                    return new ScanRecords<>(fetchRecords);
                }
            } while (System.nanoTime() - startNanos < timeoutNanos);

            return new ScanRecords<>(Collections.emptyMap());
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

    @Override
    public void subscribe(int bucket, long offset) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"subscribe(long partitionId, int bucket, long offset)\" to "
                            + "subscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            this.metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is not a partitioned table, please use "
                            + "\"subscribe(int bucket, long offset)\" to "
                            + "subscribe a non-partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            // we make assumption that the partition id must belong to the current table
            // if we can't find the partition id from the table path, we'll consider the table
            // is not exist
            this.metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "Can't unsubscribe a partition for a non-partitioned table.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void wakeup() {
        logFetcher.wakeup();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<TableBucket, List<ScanRecord<T>>> pollForFetches() {
        // Use raw types to handle the conversion between non-generic and generic types
        Map<TableBucket, List<ScanRecord>> fetchedRecords = logFetcher.collectFetch();
        if (!fetchedRecords.isEmpty()) {
            return (Map) fetchedRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();

        return (Map) logFetcher.collectFetch();
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the scanner has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (closed) {
            release();
            throw new IllegalStateException("This scanner has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this scanner from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded
     * usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get()
                && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException(
                    "Scanner is not safe for multithreaded access. "
                            + "currentThread(name: "
                            + thread.getName()
                            + ", id: "
                            + threadId
                            + ")"
                            + " otherThread(id: "
                            + currentThread.get()
                            + ")");
        }
        refCount.incrementAndGet();
    }

    /** Release the light lock protecting the consumer from multithreaded access. */
    private void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public <P> LogScanner<P> withRowSerializer(
            com.alibaba.fluss.client.row.RowSerializer<P> converter) {
        // Create a new LogScannerWithSerializer that wraps this scanner and applies the converter
        return new LogScannerWithSerializer<>(this, converter);
    }

    @Override
    public void close() {
        acquire();
        try {
            if (!closed) {
                LOG.trace("Closing log scanner for table: {}", tablePath);
                scannerMetricGroup.close();
                logFetcher.close();
            }
            LOG.debug("Log scanner for table: {} has been closed", tablePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log scanner for table " + tablePath, e);
        } finally {
            closed = true;
            release();
        }
    }

    /** A wrapper class that converts ScanRecords to use a RowConverter. */
    private static class LogScannerWithSerializer<T, P> implements LogScanner<P> {
        private final LogScanner<T> delegate;
        private final RowSerializer<P> converter;

        LogScannerWithSerializer(LogScanner<T> delegate, RowSerializer<P> converter) {
            this.delegate = delegate;
            this.converter = converter;
        }

        @Override
        public ScanRecords<P> poll(Duration timeout) {
            ScanRecords<T> records = delegate.poll(timeout);
            if (records.isEmpty()) {
                return (ScanRecords<P>) ScanRecords.EMPTY;
            }

            // Convert the records using the converter
            Map<TableBucket, List<ScanRecord<P>>> convertedRecords = new HashMap<>();
            for (TableBucket bucket : records.buckets()) {
                List<ScanRecord<P>> convertedBucketRecords = new ArrayList<>();
                for (ScanRecord<T> record : records.records(bucket)) {
                    if (record.getRow() instanceof com.alibaba.fluss.row.InternalRow) {
                        convertedBucketRecords.add(
                                ScanRecord.withRowSerializer(
                                        record.logOffset(),
                                        record.timestamp(),
                                        record.getChangeType(),
                                        record.getRow(),
                                        converter));
                    }
                }
                if (!convertedBucketRecords.isEmpty()) {
                    convertedRecords.put(bucket, convertedBucketRecords);
                }
            }

            return new ScanRecords<>(convertedRecords);
        }

        @Override
        public <Q> LogScanner<Q> withRowSerializer(RowSerializer<Q> newRowSerializer) {
            return new LogScannerWithSerializer<>(delegate, newRowSerializer);
        }

        @Override
        public void subscribe(int bucket, long offset) {
            delegate.subscribe(bucket, offset);
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            delegate.subscribe(partitionId, bucket, offset);
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
            delegate.unsubscribe(partitionId, bucket);
        }

        @Override
        public void wakeup() {
            delegate.wakeup();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
