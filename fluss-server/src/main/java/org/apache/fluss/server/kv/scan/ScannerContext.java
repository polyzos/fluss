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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.clock.Clock;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The context for a scanner session. Each instance holds a RocksDB snapshot and iterator for
 * point-in-time scan isolation. The {@link ResourceGuard.Lease} prevents RocksDB from being closed
 * while this session is active.
 *
 * <p>Most fields of this class are not thread-safe; in particular, {@link #iterator} and
 * {@link #callSeqId} must only be accessed by a single thread at a time. The {@link #expired} flag
 * is the sole exception: it is an {@link java.util.concurrent.atomic.AtomicBoolean} and may be
 * written by the TTL reaper thread while another thread holds a reference to the context.
 */
@NotThreadSafe
public class ScannerContext implements AutoCloseable {
    private final byte[] scannerId;
    private final TableBucket tableBucket;
    private final RocksDBKv rocksDBKv;
    private final RocksIterator iterator;
    private final ReadOptions readOptions;
    private final Snapshot snapshot;
    private final ResourceGuard.Lease resourceLease;

    private int callSeqId;
    private long lastAccessTime;
    private final AtomicBoolean expired = new AtomicBoolean(false);

    public ScannerContext(
            byte[] scannerId,
            TableBucket tableBucket,
            RocksDBKv rocksDBKv,
            RocksIterator iterator,
            ReadOptions readOptions,
            Snapshot snapshot,
            ResourceGuard.Lease resourceLease,
            Clock clock) {
        this.scannerId = scannerId;
        this.tableBucket = tableBucket;
        this.rocksDBKv = rocksDBKv;
        this.iterator = iterator;
        this.readOptions = readOptions;
        this.snapshot = snapshot;
        this.resourceLease = resourceLease;
        this.callSeqId = 0;
        this.lastAccessTime = clock.milliseconds();
    }

    public byte[] getScannerId() {
        return scannerId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public RocksIterator getIterator() {
        return iterator;
    }

    public int getCallSeqId() {
        return callSeqId;
    }

    public void setCallSeqId(int callSeqId) {
        this.callSeqId = callSeqId;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void updateLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    /**
     * Marks this scanner as expired by the TTL reaper. After this call, {@link #isExpired()} will
     * return {@code true}, allowing the server to distinguish an expired session from an unknown
     * scanner id.
     */
    public void markExpired() {
        expired.set(true);
    }

    public boolean isExpired() {
        return expired.get();
    }

    @Override
    public void close() {
        try {
            iterator.close();
        } finally {
            try {
                readOptions.close();
            } finally {
                try {
                    rocksDBKv.releaseSnapshot(snapshot);
                } finally {
                    resourceLease.close();
                }
            }
        }
    }
}
