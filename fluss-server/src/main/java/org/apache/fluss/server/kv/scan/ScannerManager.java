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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages scanner sessions for KV snapshot queries. Each session holds a RocksDB snapshot and
 * iterator. Sessions that are idle longer than the TTL are reaped automatically by a background
 * task.
 */
public class ScannerManager implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    private final Map<ByteBuffer, ScannerContext> scanners = MapUtils.newConcurrentHashMap();
    /**
     * Tracks recently-expired scanner IDs and their expiry timestamp. Allows distinguishing between
     * a scanner that expired (SCANNER_EXPIRED) and one that was never created (UNKNOWN_SCANNER_ID).
     * Entries are pruned by the cleanup task after {@link #recentlyExpiredRetentionMs} to bound
     * memory usage.
     */
    private final Map<ByteBuffer, Long> recentlyExpiredIds = MapUtils.newConcurrentHashMap();
    private final Scheduler scheduler;
    private final Clock clock;
    private final long scannerTtlMs;
    /** How long recently-expired IDs are retained for diagnostic error reporting (2 × TTL). */
    private final long recentlyExpiredRetentionMs;
    private final int maxPerBucket;
    private final int maxPerServer;
    private final AtomicInteger totalScanners = new AtomicInteger(0);
    private ScheduledFuture<?> cleanupTask;

    public ScannerManager(Configuration conf, Scheduler scheduler) {
        this(conf, scheduler, SystemClock.getInstance());
    }

    public ScannerManager(Configuration conf, Scheduler scheduler, Clock clock) {
        this.scheduler = scheduler;
        this.clock = clock;
        this.scannerTtlMs = conf.get(ConfigOptions.SERVER_SCANNER_TTL).toMillis();
        this.recentlyExpiredRetentionMs = 2 * scannerTtlMs;
        long expirationIntervalMs =
                conf.get(ConfigOptions.SERVER_SCANNER_EXPIRATION_INTERVAL).toMillis();
        this.maxPerBucket = conf.get(ConfigOptions.SERVER_SCANNER_MAX_PER_BUCKET);
        this.maxPerServer = conf.get(ConfigOptions.SERVER_SCANNER_MAX_PER_SERVER);
        this.cleanupTask =
                scheduler.schedule(
                        "scanner-expiration",
                        this::cleanupExpiredScanners,
                        expirationIntervalMs,
                        expirationIntervalMs);
    }

    /**
     * Creates a new scanner session for the given KV tablet and returns the {@link ScannerContext},
     * or {@code null} if the bucket is empty (no rows to scan).
     *
     * <p>When {@code null} is returned, no scanner session is registered and no limit slot is
     * consumed. The caller should return an empty response immediately.
     *
     * <p><b>Note on limit enforcement:</b> The per-bucket and server-level limit checks are not
     * atomic with the subsequent {@code scanners.put()}. This is an intentional trade-off: the
     * limits are soft guards against runaway scanner creation, not hard resource quotas. In the
     * rare race where two threads both pass the limit check before either registers, the server
     * may briefly exceed the configured maximum by a small amount.
     *
     * @throws TooManyScannersException if per-bucket or per-server limits are exceeded
     * @throws IOException if the RocksDB resource guard cannot be acquired
     */
    @Nullable
    public ScannerContext createScanner(KvTablet kvTablet, TableBucket tableBucket)
            throws IOException {
        // Check server-level limit first (cheap atomic check)
        if (totalScanners.get() >= maxPerServer) {
            throw new TooManyScannersException(
                    "Server scanner limit ("
                            + maxPerServer
                            + ") reached. Try again later.");
        }

        // Check per-bucket limit
        int bucketCount = countScannersForBucket(tableBucket);
        if (bucketCount >= maxPerBucket) {
            throw new TooManyScannersException(
                    "Per-bucket scanner limit ("
                            + maxPerBucket
                            + ") reached for bucket "
                            + tableBucket
                            + ". Try again later.");
        }

        RocksDBKv rocksDBKv = kvTablet.getRocksDBKv();
        // Acquire a lease to prevent RocksDB from being closed while iterating.
        // All subsequent resource allocations are guarded by a try/catch so the lease
        // is always released on failure.
        ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
        try {
            Snapshot snapshot = rocksDBKv.getSnapshot();
            ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
            RocksIterator iterator = rocksDBKv.newIterator(readOptions);
            iterator.seekToFirst();

            if (!iterator.isValid()) {
                // Empty bucket: release all resources without registering a scanner session.
                iterator.close();
                readOptions.close();
                rocksDBKv.releaseSnapshot(snapshot);
                lease.close();
                return null;
            }

            byte[] scannerId = generateScannerId();
            ScannerContext context =
                    new ScannerContext(
                            scannerId,
                            tableBucket,
                            rocksDBKv,
                            iterator,
                            readOptions,
                            snapshot,
                            lease,
                            clock);
            scanners.put(context.getScannerIdKey(), context);
            totalScanners.incrementAndGet();
            return context;
        } catch (Exception e) {
            lease.close();
            throw e;
        }
    }

    /**
     * Looks up a scanner by id and refreshes its last-access timestamp. Returns {@code null} if
     * the scanner is not registered (was never created or has already been removed).
     */
    @Nullable
    public ScannerContext getScanner(byte[] scannerId) {
        ScannerContext context = scanners.get(ByteBuffer.wrap(scannerId));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        }
        return context;
    }

    /**
     * Returns {@code true} if the scanner with the given id was recently expired by the TTL reaper.
     * This allows callers to distinguish a SCANNER_EXPIRED error from an UNKNOWN_SCANNER_ID error
     * when {@link #getScanner(byte[])} returns {@code null}.
     */
    public boolean isRecentlyExpired(byte[] scannerId) {
        return recentlyExpiredIds.containsKey(ByteBuffer.wrap(scannerId));
    }

    /**
     * Removes and closes the scanner identified by {@code scannerId}. No-op if the id is unknown.
     */
    public void removeScanner(byte[] scannerId) {
        ScannerContext context = scanners.remove(ByteBuffer.wrap(scannerId));
        if (context != null) {
            totalScanners.decrementAndGet();
            closeScannerContext(context);
        }
    }

    /**
     * Removes and closes a known scanner context directly, avoiding a map lookup. Prefer this over
     * {@link #removeScanner(byte[])} when the caller already holds the {@link ScannerContext}.
     */
    public void removeScanner(ScannerContext context) {
        if (scanners.remove(context.getScannerIdKey(), context)) {
            totalScanners.decrementAndGet();
            closeScannerContext(context);
        }
    }

    /**
     * Closes and removes all active scanner sessions for the given bucket. Called when a bucket
     * loses leadership to prevent stale RocksDB snapshot/iterator leaks.
     */
    public void closeScannersForBucket(TableBucket tableBucket) {
        List<ByteBuffer> toRemove = new ArrayList<>();
        for (Map.Entry<ByteBuffer, ScannerContext> entry : scanners.entrySet()) {
            if (tableBucket.equals(entry.getValue().getTableBucket())) {
                toRemove.add(entry.getKey());
            }
        }
        for (ByteBuffer key : toRemove) {
            ScannerContext context = scanners.remove(key);
            if (context != null) {
                totalScanners.decrementAndGet();
                LOG.info(
                        "Closing scanner {} for bucket {} due to leadership change.",
                        scannerIdToString(key.array()),
                        tableBucket);
                closeScannerContext(context);
            }
        }
    }

    /**
     * Counts active scanners for a given bucket. O(n) over total scanners — acceptable because
     * {@code maxPerServer} is small (default 200) and this path is only on scanner creation.
     */
    private int countScannersForBucket(TableBucket tableBucket) {
        int count = 0;
        for (ScannerContext ctx : scanners.values()) {
            if (tableBucket.equals(ctx.getTableBucket())) {
                count++;
            }
        }
        return count;
    }

    private void cleanupExpiredScanners() {
        long now = clock.milliseconds();

        // Prune stale entries from the recently-expired set to bound memory usage
        recentlyExpiredIds.entrySet().removeIf(e -> now - e.getValue() > recentlyExpiredRetentionMs);

        for (Map.Entry<ByteBuffer, ScannerContext> entry : scanners.entrySet()) {
            ScannerContext context = entry.getValue();
            if (now - context.getLastAccessTime() > scannerTtlMs) {
                // Atomic conditional remove to avoid double-close race with removeScanner()
                if (scanners.remove(entry.getKey(), context)) {
                    totalScanners.decrementAndGet();
                    // Record the expiry so subsequent lookups can return SCANNER_EXPIRED
                    recentlyExpiredIds.put(entry.getKey(), now);
                    LOG.info(
                            "Scanner {} expired after {}ms idle, closing it.",
                            scannerIdToString(entry.getKey().array()),
                            scannerTtlMs);
                    closeScannerContext(context);
                }
            }
        }
    }

    private void closeScannerContext(ScannerContext context) {
        try {
            context.close();
        } catch (Exception e) {
            LOG.error("Failed to close scanner context.", e);
        }
    }

    private byte[] generateScannerId() {
        return UUID.randomUUID().toString().replace("-", "").getBytes(StandardCharsets.UTF_8);
    }

    private String scannerIdToString(byte[] scannerId) {
        return new String(scannerId, StandardCharsets.UTF_8);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        try {
            close();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public void close() throws Exception {
        if (cleanupTask != null) {
            cleanupTask.cancel(true);
        }
        // If the cleanup task races with this loop, a scanner could be closed by both. RocksDB's
        // NativeReference.close() is safe to call twice, but ResourceGuard.Lease.close() is not —
        // it decrements a reference count. In practice this window is tiny (cancel + loop are on
        // the same thread in tests; production uses a scheduled executor where the task is
        // already done or is skipped by the cancel). This is an acceptable trade-off at shutdown.
        for (ScannerContext context : scanners.values()) {
            closeScannerContext(context);
        }
        scanners.clear();
        recentlyExpiredIds.clear();
        totalScanners.set(0);
    }
}
