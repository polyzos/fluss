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

package org.apache.fluss.server.kv.scan;

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.utils.ResourceGuard;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Server-side state for a single KV full-scan session.
 *
 * <p>A {@code ScannerContext} holds a point-in-time RocksDB {@link Snapshot}, the {@link
 * ReadOptions} pinning it, and a cursor ({@link RocksIterator}) that persists across multiple
 * batched-fetch RPCs from the same client. It also holds a {@link ResourceGuard.Lease} that
 * prevents the underlying RocksDB instance from being closed while the scan is in progress.
 *
 * <p>Instances are created by {@link org.apache.fluss.server.kv.KvTablet#openScan} and registered
 * by {@link ScannerManager}. They must be closed when the scan completes, the client requests an
 * explicit close, or the session expires due to inactivity.
 *
 * <p><b>Thread safety:</b> The iterator cursor ({@link #advance()}, {@link #isValid()}, {@link
 * #currentValue()}) must be driven by only one thread at a time. Callers that mutate cursor or
 * sequence-id state must first acquire exclusive use via {@link #tryAcquireForUse()} and release it
 * with {@link #releaseAfterUse()} when done; this prevents two concurrent client RPCs sharing a
 * scanner ID from racing the iterator (which would corrupt RocksDB state at the JNI boundary).
 * {@link #close()} is thread-safe.
 */
@NotThreadSafe
public class ScannerContext implements Closeable {
    private final String scannerId;
    private final byte[] scannerIdBytes;
    private final TableBucket tableBucket;
    private final RocksDBKv rocksDBKv;
    private final RocksIterator iterator;
    private final ReadOptions readOptions;
    private final Snapshot snapshot;
    private final ResourceGuard.Lease resourceLease;

    /**
     * Log offset of the latest record flushed to the KV store at the moment this scanner's RocksDB
     * snapshot was opened. Sent to the client on the first response so that downstream consumers
     * can perform a consistent snapshot-to-log handoff.
     */
    private final long logOffset;

    private long remainingLimit;
    // Initial value -1 so that the first client call_seq_id of 0 satisfies the server's
    // in-order check: expectedSeqId = callSeqId + 1 = -1 + 1 = 0.
    // callSeqId validation is only performed for continuation requests (those carrying a
    // scanner_id), never for the initial open request (those carrying a bucket_scan_req).
    // volatile because a continuation may be served by a different RPC worker thread than the
    // one that last advanced this counter.
    private volatile int callSeqId = -1;

    /**
     * Wall-clock timestamp (ms) of the most recent request that touched this session. Used by
     * {@link ScannerManager} for TTL-based eviction. {@code volatile} so the evictor thread cannot
     * observe a stale timestamp written by the most recent RPC worker.
     */
    private volatile long lastAccessTime;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Cursor-exclusion flag. Two concurrent {@code scanKv} RPCs sharing a scanner ID would both
     * pass the {@code callSeqId} check (each sees the same pre-mutation value) and then race on
     * {@link RocksIterator#next()}, which is unsafe at the JNI boundary. Acquiring this flag at the
     * top of the handler and releasing it in {@code finally} serialises the cursor without blocking
     * — the loser of the CAS receives an explicit "concurrent scan" error.
     */
    private final AtomicBoolean inUse = new AtomicBoolean(false);

    public ScannerContext(
            String scannerId,
            TableBucket tableBucket,
            RocksDBKv rocksDBKv,
            RocksIterator iterator,
            ReadOptions readOptions,
            Snapshot snapshot,
            ResourceGuard.Lease resourceLease,
            long limit,
            long logOffset,
            long initialAccessTimeMs) {
        this.scannerId = scannerId;
        this.scannerIdBytes = scannerId.getBytes(StandardCharsets.UTF_8);
        this.tableBucket = tableBucket;
        this.rocksDBKv = rocksDBKv;
        this.iterator = iterator;
        this.readOptions = readOptions;
        this.snapshot = snapshot;
        this.resourceLease = resourceLease;
        this.remainingLimit = limit <= 0 ? -1L : limit;
        this.logOffset = logOffset;
        this.lastAccessTime = initialAccessTimeMs;
    }

    /**
     * Returns the log offset captured at the moment this scanner's RocksDB snapshot was opened. See
     * {@link #logOffset}.
     */
    public long getLogOffset() {
        return logOffset;
    }

    public byte[] getScannerId() {
        return scannerIdBytes;
    }

    /**
     * Returns the scanner ID as a UTF-8 {@link String}. Package-private: used by {@link
     * ScannerManager} as the key in its internal {@code scanners} map. The wire-format
     * representation is always {@link #getScannerId()} (raw bytes).
     */
    String getIdString() {
        return scannerId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public boolean isValid() {
        return iterator.isValid() && remainingLimit != 0;
    }

    public byte[] currentValue() {
        return iterator.value();
    }

    /**
     * Advances the cursor by one entry and decrements the remaining-rows limit if applicable. Must
     * only be called when {@link #isValid()} returns {@code true}.
     */
    public void advance() {
        iterator.next();
        if (remainingLimit > 0) {
            remainingLimit--;
        }
    }

    /**
     * Validates the underlying RocksDB iterator's status. {@link RocksIterator#next()} does not
     * throw on RocksDB-internal errors — the iterator silently transitions to invalid and the error
     * is carried by {@link RocksIterator#status()}. Callers MUST invoke this once iteration has
     * stopped (i.e. {@link #isValid()} returned {@code false}) so that a clean end-of-range is
     * distinguishable from an error path; otherwise an internal failure would silently truncate the
     * scan and the client would conclude the scan completed when in fact rows were dropped.
     *
     * @throws KvStorageException if the iterator reports an internal error
     */
    public void checkIteratorStatus() {
        try {
            iterator.status();
        } catch (RocksDBException e) {
            throw new KvStorageException(
                    "RocksDB iterator error during scan for bucket " + tableBucket, e);
        }
    }

    /** Returns the call-sequence ID of the last successfully served request, or {@code -1}. */
    public int getCallSeqId() {
        return callSeqId;
    }

    /**
     * Updates the call-sequence ID. Must be called <em>after</em> the response payload has been
     * fully prepared, so that a client retry with the same {@code callSeqId} can be detected.
     */
    public void setCallSeqId(int callSeqId) {
        this.callSeqId = callSeqId;
    }

    /** Resets the idle-TTL timer to the given wall-clock time. */
    public void updateLastAccessTime(long nowMs) {
        this.lastAccessTime = nowMs;
    }

    /**
     * Returns {@code true} if the session has been idle for longer than {@code ttlMs}, based on the
     * provided current time.
     */
    public boolean isExpired(long ttlMs, long nowMs) {
        return nowMs - lastAccessTime > ttlMs;
    }

    /**
     * Atomically tries to claim exclusive use of this context's cursor and sequence-id state.
     * Returns {@code true} if the caller now holds the exclusive-use flag and may safely advance
     * the iterator and update {@link #setCallSeqId(int)}; {@code false} if another thread is
     * already inside a {@code scanKv} call for this scanner ID.
     *
     * <p>Callers MUST pair every successful acquire with a {@link #releaseAfterUse()} in a {@code
     * finally} block.
     */
    public boolean tryAcquireForUse() {
        return inUse.compareAndSet(false, true);
    }

    /**
     * Releases the exclusive-use flag obtained via {@link #tryAcquireForUse()}. Calling this on a
     * context that has already been {@link #close()}d is harmless: the context is no longer
     * reachable through {@link ScannerManager}, so no other thread will observe the flag.
     */
    public void releaseAfterUse() {
        inUse.set(false);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                iterator.close();
            } finally {
                try {
                    readOptions.close();
                } finally {
                    try {
                        // Wrap releaseSnapshot in its own try/finally so that snapshot.close()
                        // always runs even if releaseSnapshot throws — otherwise the native
                        // snapshot handle would leak while the ResourceGuard.Lease is still
                        // released, masking the leak from later kvTablet.close() shutdown checks.
                        try {
                            rocksDBKv.getDb().releaseSnapshot(snapshot);
                        } finally {
                            snapshot.close();
                        }
                    } finally {
                        resourceLease.close();
                    }
                }
            }
        }
    }
}
