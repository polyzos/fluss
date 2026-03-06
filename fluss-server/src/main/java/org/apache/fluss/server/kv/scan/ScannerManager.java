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
    private final Scheduler scheduler;
    private final Clock clock;
    private final long scannerTtlMs;
    private final long expirationIntervalMs;
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
        this.expirationIntervalMs =
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
     * Creates a new scanner session for the given KV tablet and returns the {@link ScannerContext}.
     * The context contains the scanner id assigned to this session.
     *
     * @throws TooManyScannersException if per-bucket or per-server limits are exceeded
     * @throws IOException if the RocksDB resource guard cannot be acquired
     */
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
        // Acquire a lease to prevent RocksDB from being closed while iterating
        ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
        Snapshot snapshot = rocksDBKv.getSnapshot();
        ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
        RocksIterator iterator = rocksDBKv.newIterator(readOptions);
        iterator.seekToFirst();

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
        scanners.put(ByteBuffer.wrap(scannerId), context);
        totalScanners.incrementAndGet();
        return context;
    }

    /**
     * Looks up a scanner by id and refreshes its last-access timestamp. Returns {@code null} if
     * the scanner is not registered (was never created or has already been removed).
     */
    public ScannerContext getScanner(byte[] scannerId) {
        ScannerContext context = scanners.get(ByteBuffer.wrap(scannerId));
        if (context != null) {
            context.updateLastAccessTime(clock.milliseconds());
        }
        return context;
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
        for (Map.Entry<ByteBuffer, ScannerContext> entry : scanners.entrySet()) {
            ScannerContext context = entry.getValue();
            if (now - context.getLastAccessTime() > scannerTtlMs) {
                // Mark expired before removal so TabletService can distinguish error codes
                context.markExpired();
                // Atomic conditional remove to avoid double-close race with removeScanner()
                if (scanners.remove(entry.getKey(), context)) {
                    totalScanners.decrementAndGet();
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
        for (ScannerContext context : scanners.values()) {
            closeScannerContext(context);
        }
        scanners.clear();
        totalScanners.set(0);
    }
}
