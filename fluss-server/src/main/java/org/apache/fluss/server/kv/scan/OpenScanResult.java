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

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Result of opening a KV full-scan session, returned by {@link
 * org.apache.fluss.server.kv.KvTablet#openScan}, {@link
 * org.apache.fluss.server.replica.Replica#openScan}, and {@link ScannerManager#createScanner}.
 *
 * <p>Carries both the (optional) {@link ScannerContext} and the log offset captured under the same
 * lock as the RocksDB snapshot. The log offset must reach the client even on the empty-bucket fast
 * path so that a snapshot-to-log handoff can be performed without missing or duplicating records.
 */
@Internal
public final class OpenScanResult {

    @Nullable private final ScannerContext context;
    private final long logOffset;

    public OpenScanResult(@Nullable ScannerContext context, long logOffset) {
        this.context = context;
        this.logOffset = logOffset;
    }

    /**
     * Returns the registered {@link ScannerContext}, or {@code null} if the bucket was empty at
     * snapshot time (no session is created in that case).
     */
    @Nullable
    public ScannerContext getContext() {
        return context;
    }

    /**
     * Returns the log offset of the latest record that was flushed into the KV store at the moment
     * the RocksDB snapshot was opened. Always non-negative.
     */
    public long getLogOffset() {
        return logOffset;
    }
}
