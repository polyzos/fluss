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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv.ScanIteratorHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages server-side {@link KvScanIterator} sessions for streaming KV scans.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Registering new scan sessions and assigning them a unique scanner ID.
 *   <li>Routing fetch and keep-alive requests to the correct session by scanner ID.
 *   <li>Expiring idle sessions whose last access time exceeds the configured TTL.
 *   <li>Forcibly closing all sessions for a given {@link TableBucket} when the replica is stopped,
 *       so that the underlying RocksDB resources are released before the tablet closes.
 * </ul>
 *
 * <p>Thread-safety: the scanner map uses a {@link ConcurrentHashMap}; individual scanners are
 * themselves thread-safe.
 */
public class ScannerManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ScannerManager.class);

    /** Maps scanner ID → KvScanIterator. */
    private final ConcurrentHashMap<String, KvScanIterator> scanners = new ConcurrentHashMap<>();

    /**
     * Maps scanner ID → TableBucket so we can efficiently close all scanners for a given bucket
     * when a replica is stopped.
     */
    private final ConcurrentHashMap<String, TableBucket> scannerBucket = new ConcurrentHashMap<>();

    private final long ttlMs;
    private final ScheduledExecutorService expirationScheduler;

    public ScannerManager(long ttlMs, long expirationIntervalMs) {
        this.ttlMs = ttlMs;
        this.expirationScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> {
                    Thread t = new Thread(r, "kv-scanner-expiration");
                    t.setDaemon(true);
                    return t;
                });
        expirationScheduler.scheduleAtFixedRate(
                this::expireIdleScanners,
                expirationIntervalMs,
                expirationIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Registers a new scan session and returns the assigned scanner ID (UUID hex string).
     *
     * @param tableBucket the source bucket (used for bulk-close on replica stop)
     * @param logOffset   the log offset at scanner creation time
     * @param rowLimit    maximum rows to return; 0 means unlimited
     * @param handle      the RocksDB snapshot handle, positioned at seekToFirst
     * @return the newly assigned scanner ID
     */
    public String registerScanner(
            TableBucket tableBucket, long logOffset, long rowLimit, ScanIteratorHandle handle) {
        String scannerId = UUID.randomUUID().toString().replace("-", "");
        KvScanIterator scanner = new KvScanIterator(scannerId, logOffset, rowLimit, handle);
        scanners.put(scannerId, scanner);
        scannerBucket.put(scannerId, tableBucket);
        LOG.debug("Registered KV scanner {} for bucket {}", scannerId, tableBucket);
        return scannerId;
    }

    /**
     * Looks up the scanner session for {@code scannerId}.
     *
     * @throws IllegalArgumentException if the scanner does not exist (expired or never created)
     */
    public KvScanIterator getScanner(String scannerId) {
        KvScanIterator scanner = scanners.get(scannerId);
        if (scanner == null) {
            throw new IllegalArgumentException(
                    "Scanner " + scannerId + " does not exist or has expired.");
        }
        return scanner;
    }

    /**
     * Removes and closes the scanner session identified by {@code scannerId}.
     * No-op if the scanner does not exist.
     */
    public void closeScanner(String scannerId) {
        KvScanIterator scanner = scanners.remove(scannerId);
        scannerBucket.remove(scannerId);
        if (scanner != null) {
            scanner.close();
            LOG.debug("Closed KV scanner {}", scannerId);
        }
    }

    /**
     * Forcibly closes and removes all scanners associated with the given {@link TableBucket}.
     * Called when a replica is stopped so that RocksDB resources are released before the tablet
     * is closed.
     */
    public void closeScannersForBucket(TableBucket tableBucket) {
        List<String> toClose = new ArrayList<>();
        scannerBucket.forEach((id, bucket) -> {
            if (tableBucket.equals(bucket)) {
                toClose.add(id);
            }
        });
        for (String id : toClose) {
            closeScanner(id);
        }
        if (!toClose.isEmpty()) {
            LOG.info(
                    "Closed {} KV scanner(s) for stopped bucket {}",
                    toClose.size(),
                    tableBucket);
        }
    }

    /** Background task: expire scanners that have been idle longer than {@code ttlMs}. */
    private void expireIdleScanners() {
        List<String> expired = new ArrayList<>();
        scanners.forEach((id, scanner) -> {
            if (scanner.isExpired(ttlMs)) {
                expired.add(id);
            }
        });
        for (String id : expired) {
            LOG.info("Expiring idle KV scanner {}", id);
            closeScanner(id);
        }
    }

    @Override
    public void close() {
        expirationScheduler.shutdownNow();
        // Close all remaining scanners
        new ArrayList<>(scanners.keySet()).forEach(this::closeScanner);
    }
}
