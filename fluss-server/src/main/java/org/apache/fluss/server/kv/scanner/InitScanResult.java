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

/**
 * Holds the two values returned when a new streaming KV scan is initialised: the log offset at
 * the time the RocksDB snapshot was taken, and the {@link ScanIteratorHandle} itself.
 */
public final class InitScanResult {

    /** The log high-watermark captured at the moment the RocksDB snapshot was taken. */
    public final long logOffset;

    /** The RocksDB snapshot + iterator handle, positioned at {@code seekToFirst()}. */
    public final ScanIteratorHandle handle;

    public InitScanResult(long logOffset, ScanIteratorHandle handle) {
        this.logOffset = logOffset;
        this.handle = handle;
    }
}
