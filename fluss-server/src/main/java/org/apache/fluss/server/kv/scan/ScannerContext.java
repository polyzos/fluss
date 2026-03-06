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

/**
 * The context for a scanner session. Each instance holds a RocksDB snapshot and iterator for
 * point-in-time scan isolation. The {@link ResourceGuard.Lease} prevents RocksDB from being closed
 * while this session is active.
 *
 * <p>This class is not thread-safe; all fields must only be accessed by a single thread at a time.
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
