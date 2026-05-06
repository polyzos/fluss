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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.AutoCloseableAsync;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages server-side KV full-scan sessions ({@link ScannerContext}).
 *
 * <p>Each KV full scan opens a persistent server-side session that holds a point-in-time RocksDB
 * snapshot and a cursor. Sessions are keyed by a server-assigned UUID-based scanner ID and persist
 * across multiple batched-fetch RPCs from the same client.
 *
 * <h3>Concurrency limits</h3>
 *
 * <ul>
 *   <li><b>Per-bucket:</b> at most {@code maxPerBucket} concurrent sessions on any single bucket.
 *   <li><b>Per-server:</b> at most {@code maxPerServer} concurrent sessions across all buckets.
 * </ul>
 *
 * <p>Limit enforcement is two-phase: a fast pre-check guards the common case; the subsequent atomic
 * increment with re-check and rollback prevents the TOCTOU race from permanently breaching the
 * configured limits. Exceeding either limit causes {@link TooManyScannersException}.
 *
 * <h3>Empty bucket handling</h3>
 *
 * <p>If the target bucket contains no rows at the time the scan is opened, {@link
 * #createScanner(KvTablet, TableBucket, Long)} returns {@code null} without consuming a limit slot.
 * The caller should return an empty response immediately.
 *
 * <h3>TTL eviction</h3>
 *
 * <p>A background evictor task runs every {@code kv.scanner.expiration-interval} and removes
 * sessions idle longer than {@code kv.scanner.ttl}. Recently evicted IDs are retained for {@code 2
 * × ttl} so callers can distinguish "expired" from "never existed."
 *
 * <h3>Leadership change</h3>
 *
 * {@link #closeScannersForBucket(TableBucket)} must be called when a bucket loses leadership to
 * release all RocksDB snapshot/iterator resources for that bucket promptly.
 */
public class ScannerManager implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    private final Map<String, ScannerContext> scanners = new ConcurrentHashMap<>();
    private final Map<String, Long> recentlyExpiredIds = new ConcurrentHashMap<>();

    /** Per-bucket active scanner count, used for O(1) per-bucket limit enforcement. */
    private final Map<TableBucket, AtomicInteger> perBucketCount = new ConcurrentHashMap<>();

    /** Total active scanner count across all buckets on this tablet server. */
    private final AtomicInteger totalScanners = new AtomicInteger(0);

    private final Clock clock;
    private final long scannerTtlMs;
    private final long recentlyExpiredRetentionMs;
    private final int maxPerBucket;
    private final int maxPerServer;

    @Nullable private ScheduledFuture<?> evictorTask;

    public ScannerManager(Configuration conf, Scheduler scheduler) {
        this(conf, scheduler, SystemClock.getInstance());
    }

    @VisibleForTesting
    ScannerManager(Configuration conf, Scheduler scheduler, Clock clock) {
        this.clock = clock;
        this.scannerTtlMs = conf.get(ConfigOptions.KV_SCANNER_TTL).toMillis();
        this.recentlyExpiredRetentionMs = 2 * scannerTtlMs;
        this.maxPerBucket = conf.get(ConfigOptions.KV_SCANNER_MAX_PER_BUCKET);
        this.maxPerServer = conf.get(ConfigOptions.KV_SCANNER_MAX_PER_SERVER);

        long expirationIntervalMs =
                conf.get(ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL).toMillis();
        this.evictorTask =
                scheduler.schedule(
                        "scanner-expiration",
                        this::evictExpiredScanners,
                        expirationIntervalMs,
                        expirationIntervalMs);

        LOG.info(
                "Started ScannerManager: ttl={}ms, expirationInterval={}ms, "
                        + "maxPerBucket={}, maxPerServer={}",
                scannerTtlMs,
                expirationIntervalMs,
                maxPerBucket,
                maxPerServer);
    }

    /**
     * Creates a new scan session for the given replica, taking a point-in-time RocksDB snapshot
     * under the replica's leader-ISR read lock.
     *
     * <p>Snapshot opening, leadership validation, and session registration all happen while the
     * replica's leader-ISR lock is held, so a concurrent leadership change cannot race with this
     * operation: either the scanner is registered before the leader flips (and is then evicted by
     * {@link #closeScannersForBucket(TableBucket)} during {@code makeFollowers} / {@code
     * stopReplica}), or the leadership check fails and no scanner is ever created.
     *
     * <p>Returns {@code null} if the bucket is empty (no rows to scan). In that case no session
     * slot is consumed and the caller should return an empty response immediately.
     *
     * <p><b>Limit enforcement is two-phase:</b> a fast pre-check guards the common case; the
     * subsequent atomic increment + re-check prevents the TOCTOU race from permanently breaching
     * configured limits. If registration fails after the snapshot is already opened, the context
     * is closed and the exception is re-thrown to avoid leaking resources.
     *
     * @param replica the leader {@link Replica} for the bucket being scanned
     * @param limit optional row-count limit ({@code null} or ≤ 0 means unlimited)
     * @return the newly registered {@link ScannerContext}, or {@code null} if the bucket is empty
     * @throws TooManyScannersException if the per-bucket or per-server limit is exceeded
     * @throws IOException if the underlying {@link org.apache.fluss.server.utils.ResourceGuard} is
     *     already closed (the bucket is shutting down)
     */
    @Nullable
    public ScannerContext createScanner(Replica replica, @Nullable Long limit) throws IOException {
        checkLimits(replica.getTableBucket());
        String scannerId = generateScannerId();
        return replica.openScan(this, scannerId, limit != null ? limit : -1L, clock.milliseconds());
    }

    /**
     * Atomically registers an already-opened {@link ScannerContext}, enforcing the per-bucket and
     * per-server limits. On {@link TooManyScannersException} the caller is responsible for closing
     * the context to release the underlying RocksDB resources.
     *
     * <p>Called by {@link Replica#openScan} while the leader-ISR read lock is held so that
     * registration cannot race with a leadership change.
     */
    public void register(ScannerContext context) {
        registerContext(context);
    }

    /**
     * Looks up an existing scanner session by its raw ID bytes and refreshes its last-access
     * timestamp.
     *
     * @return the {@link ScannerContext}, or {@code null} if not found (may have expired or never
     *     existed)
     */
    @Nullable
    public ScannerContext getScanner(byte[] scannerId) {
        ScannerContext context = scanners.get(new String(scannerId, StandardCharsets.UTF_8));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        }
        return context;
    }

    /**
     * Returns {@code true} if the given scanner ID belongs to a session that was recently evicted
     * by the TTL evictor (within the last {@code 2 × ttlMs}).
     *
     * <p>Callers can use this to distinguish "scanner expired" from "unknown scanner ID."
     */
    public boolean isRecentlyExpired(byte[] scannerId) {
        return recentlyExpiredIds.containsKey(new String(scannerId, StandardCharsets.UTF_8));
    }

    /**
     * Removes and closes a known scanner context directly, avoiding a map lookup.
     *
     * <p>Uses a conditional remove ({@link java.util.concurrent.ConcurrentHashMap#remove(Object,
     * Object)}) so that concurrent calls — e.g. from the TTL evictor and a close-scanner RPC
     * arriving simultaneously — result in exactly one winner closing the context, preventing
     * double-release of the non-idempotent {@link
     * org.apache.fluss.server.utils.ResourceGuard.Lease}.
     */
    public void removeScanner(ScannerContext context) {
        if (scanners.remove(context.getIdString(), context)) {
            decrementCounts(context.getTableBucket());
            closeScannerContext(context);
        }
    }

    /**
     * Looks up and removes a scanner session by its raw ID bytes.
     *
     * <p>Delegates to {@link #removeScanner(ScannerContext)} to ensure a conditional {@link
     * java.util.concurrent.ConcurrentHashMap#remove(Object, Object)} is used, which prevents a
     * double-decrement of {@code perBucketCount} when the TTL evictor races with an explicit close
     * request for the same scanner.
     *
     * <p>No-op if the ID is not found (already removed or expired).
     */
    public void removeScanner(byte[] scannerId) {
        String key = new String(scannerId, StandardCharsets.UTF_8);
        ScannerContext context = scanners.get(key);
        if (context != null) {
            removeScanner(context);
        }
    }

    /** Returns the total number of active scanner sessions on this tablet server. */
    @VisibleForTesting
    public int activeScannerCount() {
        return totalScanners.get();
    }

    /** Returns the number of active scanner sessions for the given bucket. */
    @VisibleForTesting
    public int activeScannerCountForBucket(TableBucket tableBucket) {
        AtomicInteger count = perBucketCount.get(tableBucket);
        return count == null ? 0 : count.get();
    }

    /**
     * Closes and removes all active scanner sessions for the given bucket. Must be called when a
     * bucket loses leadership to prevent stale RocksDB snapshot/iterator leaks.
     */
    public void closeScannersForBucket(TableBucket tableBucket) {
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (tableBucket.equals(entry.getValue().getTableBucket())) {
                toRemove.add(entry.getKey());
            }
        }
        for (String key : toRemove) {
            ScannerContext context = scanners.get(key);
            if (context != null) {
                LOG.info(
                        "Closing scanner {} for bucket {} due to leadership change.",
                        key,
                        tableBucket);
                removeScanner(context);
            }
        }
    }

    /**
     * Fast pre-check of per-server and per-bucket limits before opening the snapshot. This is a
     * best-effort check; a small race window exists between the check and {@link #registerContext}.
     * The race is handled by the atomic re-check inside {@link #registerContext}.
     */
    private void checkLimits(TableBucket tableBucket) {
        if (totalScanners.get() >= maxPerServer) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }
        AtomicInteger bucketCount =
                perBucketCount.computeIfAbsent(tableBucket, k -> new AtomicInteger(0));
        if (bucketCount.get() >= maxPerBucket) {
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }
    }

    /**
     * Atomically increments the counters and puts the context in the map. Throws {@link
     * TooManyScannersException} and rolls back the increments if a concurrent create caused either
     * limit to be exceeded between the initial check and this call.
     */
    private void registerContext(ScannerContext context) {
        TableBucket tableBucket = context.getTableBucket();
        AtomicInteger bucketCount =
                perBucketCount.computeIfAbsent(tableBucket, k -> new AtomicInteger(0));

        int newTotal = totalScanners.incrementAndGet();
        if (newTotal > maxPerServer) {
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: server-wide limit of %d reached.",
                            tableBucket, maxPerServer));
        }

        int newBucketCount = bucketCount.incrementAndGet();
        if (newBucketCount > maxPerBucket) {
            bucketCount.decrementAndGet();
            totalScanners.decrementAndGet();
            throw new TooManyScannersException(
                    String.format(
                            "Cannot create scanner for bucket %s: per-bucket limit of %d reached.",
                            tableBucket, maxPerBucket));
        }

        scanners.put(context.getIdString(), context);

        LOG.debug(
                "Registered scanner {} for bucket {} (total={}, perBucket={})",
                context.getIdString(),
                tableBucket,
                newTotal,
                newBucketCount);
    }

    /** TTL evictor — invoked periodically by the background scheduler. */
    private void evictExpiredScanners() {
        long now = clock.milliseconds();

        // Prune stale entries from the recently-expired cache to bound memory usage.
        recentlyExpiredIds
                .entrySet()
                .removeIf(e -> now - e.getValue() > recentlyExpiredRetentionMs);

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            ScannerContext context = entry.getValue();
            if (context.isExpired(scannerTtlMs, now)) {
                // Conditional remove prevents double-close if removeScanner() fires concurrently.
                if (scanners.remove(entry.getKey(), context)) {
                    LOG.info(
                            "Evicted idle scanner {} for bucket {} (idle > {}ms).",
                            entry.getKey(),
                            context.getTableBucket(),
                            scannerTtlMs);
                    recentlyExpiredIds.put(entry.getKey(), now);
                    decrementCounts(context.getTableBucket());
                    closeScannerContext(context);
                }
            }
        }
    }

    private void decrementCounts(TableBucket bucket) {
        totalScanners.decrementAndGet();
        perBucketCount.computeIfPresent(
                bucket,
                (k, count) -> {
                    int remaining = count.decrementAndGet();
                    return remaining <= 0 ? null : count;
                });
    }

    private void closeScannerContext(ScannerContext context) {
        try {
            context.close();
        } catch (Exception e) {
            LOG.warn(
                    "Error closing scanner {} for bucket {}.",
                    context.getIdString(),
                    context.getTableBucket(),
                    e);
        }
    }

    private static String generateScannerId() {
        return UUID.randomUUID().toString().replace("-", "");
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
    public void close() {
        // Note: we cancel but do not join the evictor. The evictor may still be mid-iteration
        // when close() begins. This is safe because (a) scanners is a ConcurrentHashMap, and
        // (b) both shutdown and the evictor use conditional remove(key, value) to mutate it,
        // so at most one side ever closes a given ScannerContext.
        if (evictorTask != null) {
            evictorTask.cancel(false);
            evictorTask = null;
        }

        for (Map.Entry<String, ScannerContext> entry : scanners.entrySet()) {
            if (scanners.remove(entry.getKey(), entry.getValue())) {
                decrementCounts(entry.getValue().getTableBucket());
                closeScannerContext(entry.getValue());
            }
        }

        // Note: totalScanners and perBucketCount are not forcibly reset here. Because both
        // shutdown and the evictor use conditional remove(key, value), each scanner is
        // decremented exactly once, so the counters naturally reach zero. A forced reset
        // would risk driving counters negative if the evictor wins a remove during close().
        recentlyExpiredIds.clear();
    }
}
