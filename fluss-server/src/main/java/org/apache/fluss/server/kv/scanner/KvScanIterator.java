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

package org.apache.fluss.server.kv.scanner;

import org.apache.fluss.server.kv.rocksdb.RocksDBKv.ScanIteratorHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a single server-side KV scan session. It holds the RocksDB {@link ScanIteratorHandle}
 * (snapshot + iterator) and tracks the metadata needed for sequence-ID validation, retry caching,
 * and TTL-based expiration.
 *
 * <p>All public methods are thread-safe.
 */
@ThreadSafe
public class KvScanIterator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvScanIterator.class);

    /** The scanner identifier (UUID hex string) assigned by ScannerManager. */
    private final String scannerId;

    /** The log offset at the time this scanner was created (returned in the first response). */
    private final long logOffset;

    /** Optional row limit; 0 means unlimited. */
    private final long rowLimit;

    /** The handle wrapping the RocksDB snapshot + ReadOptions + RocksIterator. */
    private final ScanIteratorHandle handle;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The next call_seq_id the server expects from the client.
     * Starts at 0 for the initial request.
     */
    @GuardedBy("lock")
    private int nextExpectedSeqId = 0;

    /**
     * The cached serialised records bytes from the last successful fetch, used to serve retries
     * (client sends the same call_seq_id again). Null until the first page has been served.
     */
    @GuardedBy("lock")
    @Nullable
    private byte[] lastServedRecords = null;

    /** Whether the last page had hasMoreResults = false (iterator exhausted or limit reached). */
    @GuardedBy("lock")
    private boolean lastHasMore = true;

    /** Total number of rows yielded so far (for limit enforcement). */
    @GuardedBy("lock")
    private long rowsYielded = 0;

    /** Wall-clock ms of the last access; updated on every fetch / keepAlive. */
    private volatile long lastAccessTimeMs;

    @GuardedBy("lock")
    private boolean closed = false;

    public KvScanIterator(
            String scannerId, long logOffset, long rowLimit, ScanIteratorHandle handle) {
        this.scannerId = scannerId;
        this.logOffset = logOffset;
        this.rowLimit = rowLimit;
        this.handle = handle;
        this.lastAccessTimeMs = System.currentTimeMillis();
    }

    public String getScannerId() {
        return scannerId;
    }

    public long getLogOffset() {
        return logOffset;
    }

    /**
     * Records that the keep-alive RPC was received, resetting the TTL clock.
     */
    public void keepAlive() {
        lastAccessTimeMs = System.currentTimeMillis();
    }

    /**
     * Returns {@code true} if this scanner has been inactive longer than {@code ttlMs}.
     */
    public boolean isExpired(long ttlMs) {
        return System.currentTimeMillis() - lastAccessTimeMs > ttlMs;
    }

    /**
     * Fetches the next page of records from the RocksDB iterator, up to {@code maxBytes} bytes.
     *
     * <p>Sequence-ID contract:
     * <ul>
     *   <li>If {@code callSeqId == nextExpectedSeqId}: process normally, advance iterator.
     *   <li>If {@code callSeqId == nextExpectedSeqId - 1}: this is a retry of the last request;
     *       return the previously cached response without advancing the iterator.
     *   <li>Otherwise: return an {@link IllegalStateException} indicating an unexpected seq_id.
     * </ul>
     *
     * @param callSeqId      the call_seq_id from the client request
     * @param maxBytes       maximum serialised bytes to include in the response
     * @param closeAfter     if {@code true} the scanner is closed after building the response
     * @return a {@link FetchResult} containing the records bytes and hasMore flag
     */
    public FetchResult fetch(int callSeqId, int maxBytes, boolean closeAfter) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException(
                        "Scanner " + scannerId + " is already closed.");
            }

            lastAccessTimeMs = System.currentTimeMillis();

            int expectedRetrySeqId = nextExpectedSeqId - 1;
            if (callSeqId == expectedRetrySeqId && lastServedRecords != null) {
                // Retry of the previous request — return cached response
                LOG.debug(
                        "Scanner {} received retry request with seq_id={}, returning cached response.",
                        scannerId,
                        callSeqId);
                return new FetchResult(lastServedRecords, lastHasMore);
            }

            if (callSeqId != nextExpectedSeqId) {
                throw new IllegalStateException(
                        String.format(
                                "Scanner %s received unexpected call_seq_id=%d, expected=%d. "
                                        + "The scan may have missed a chunk. Please restart the scan.",
                                scannerId,
                                callSeqId,
                                nextExpectedSeqId));
            }

            // Normal path: collect records up to maxBytes or limit
            byte[] records = collectRecords(maxBytes);
            boolean hasMore = handle.iterator.isValid()
                    && (rowLimit == 0 || rowsYielded < rowLimit);

            if (closeAfter || !hasMore) {
                hasMore = false;
                closeInternal();
            }

            // Cache for potential retry
            lastServedRecords = records;
            lastHasMore = hasMore;
            nextExpectedSeqId++;

            return new FetchResult(records, hasMore);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads value bytes from the RocksDB iterator, accumulating until {@code maxBytes} is reached
     * or the iterator is exhausted (or the row limit is hit). Returns a serialised
     * {@code DefaultValueRecordBatch} byte array, or an empty array if no values are available.
     */
    @GuardedBy("lock")
    private byte[] collectRecords(int maxBytes) {
        // Build a DefaultValueRecordBatch incrementally
        org.apache.fluss.record.DefaultValueRecordBatch.Builder builder =
                org.apache.fluss.record.DefaultValueRecordBatch.builder();

        int accumulated = 0;
        org.rocksdb.RocksIterator it = handle.iterator;

        while (it.isValid()) {
            if (rowLimit > 0 && rowsYielded >= rowLimit) {
                break;
            }
            byte[] value = it.value();
            if (accumulated > 0 && accumulated + value.length > maxBytes) {
                // Would exceed budget — stop here (always include at least one record)
                break;
            }
            builder.append(value);
            accumulated += value.length;
            rowsYielded++;
            it.next();

            if (accumulated >= maxBytes) {
                break;
            }
        }

        if (accumulated == 0) {
            return new byte[0];
        }
        org.apache.fluss.record.DefaultValueRecordBatch batch = builder.build();
        // Convert to bytes
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(batch.sizeInBytes());
        batch.writeTo(buf);
        return buf.array();
    }

    @GuardedBy("lock")
    private void closeInternal() {
        if (!closed) {
            closed = true;
            handle.close();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closeInternal();
        } finally {
            lock.unlock();
        }
    }

    /** Immutable result of a single {@link #fetch} call. */
    public static final class FetchResult {
        /** Serialised {@code DefaultValueRecordBatch} bytes, possibly empty. */
        public final byte[] records;
        /** {@code true} when the scanner has more rows to offer. */
        public final boolean hasMore;

        FetchResult(byte[] records, boolean hasMore) {
            this.records = records;
            this.hasMore = hasMore;
        }
    }
}
