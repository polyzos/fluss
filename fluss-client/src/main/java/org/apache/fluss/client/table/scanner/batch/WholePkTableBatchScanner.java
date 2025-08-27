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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

/**
 * A BatchScanner that reads the entire current data of a primary key table across all buckets.
 *
 * <p>This scanner reads the latest snapshots of all buckets and iterates through their data.
 * It enforces a configurable max rows cap via client.whole-pk-table.max-rows to prevent scanning
 * overly large tables.
 *
 * <p>Note: This first version only reads snapshot contents. Users can then read incremental
 * changes since the snapshot using LogScanner starting from the logOffsets in KvSnapshots.
 */
@Internal
public class WholePkTableBatchScanner implements BatchScanner {

    private final TableInfo tableInfo;
    private final Admin admin;
    private final RemoteFileDownloader remoteFileDownloader;
    private final String scannerTmpDir;

    private final int maxRows;

    private final Deque<KvSnapshotBatchScanner> scanners = new ArrayDeque<>();

    private int emittedRows = 0;
    private boolean initialized = false;
    private boolean endOfInput = false;

    @Nullable private CloseableIterator<InternalRow> currentIterator;

    public WholePkTableBatchScanner(
            FlussConnection conn,
            TableInfo tableInfo,
            Admin admin,
            RemoteFileDownloader remoteFileDownloader) {
        this.tableInfo = tableInfo;
        this.admin = admin;
        this.remoteFileDownloader = remoteFileDownloader;
        this.scannerTmpDir = conn.getConfiguration().getString(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR);
        this.maxRows = conn.getConfiguration().getInt(ConfigOptions.CLIENT_WHOLE_PK_TABLE_MAX_ROWS);
    }

    private void init() throws IOException {
        if (initialized) {
            return;
        }
        initialized = true;
        final KvSnapshots kvSnapshots;
        try {
            kvSnapshots = admin.getLatestKvSnapshots(tableInfo.getTablePath()).get();
        } catch (Exception e) {
            throw new IOException("Failed to get latest kv snapshots for whole-table scan", e);
        }
        for (int bucketId : kvSnapshots.getBucketIds()) {
            OptionalLong snapshotIdOpt = kvSnapshots.getSnapshotId(bucketId);
            if (!snapshotIdOpt.isPresent()) {
                // no snapshot for this bucket, skip snapshot scanning
                continue;
            }
            long snapshotId = snapshotIdOpt.getAsLong();
            TableBucket bucket = new TableBucket(tableInfo.getTableId(), kvSnapshots.getPartitionId(), bucketId);
            final KvSnapshotMetadata meta;
            try {
                meta = admin.getKvSnapshotMetadata(bucket, snapshotId).get();
            } catch (Exception e) {
                throw new IOException("Failed to get kv snapshot metadata for bucket " + bucketId, e);
            }
            List<FsPathAndFileName> files = meta.getSnapshotFiles();
            if (files == null || files.isEmpty()) {
                continue;
            }
            KvSnapshotBatchScanner scanner = new KvSnapshotBatchScanner(
                    tableInfo.getRowType(),
                    bucket,
                    files,
                    null,
                    scannerTmpDir,
                    tableInfo.getTableConfig().getKvFormat(),
                    remoteFileDownloader);
            scanners.add(scanner);
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        init();
        // ensure current iterator is available
        while (currentIterator == null || !currentIterator.hasNext()) {
            if (currentIterator != null) {
                currentIterator.close();
                currentIterator = null;
            }
            KvSnapshotBatchScanner next = scanners.poll();
            if (next == null) {
                endOfInput = true;
                return null;
            }
            currentIterator = next.pollBatch(timeout);
            if (currentIterator == null) {
                // this bucket has no data; continue to next
                continue;
            }
        }

        List<InternalRow> batch = new ArrayList<>();
        while (currentIterator.hasNext()) {
            if (emittedRows >= maxRows) {
                // hit protection
                close();
                throw new IOException(
                        "Whole primary-key table scan exceeds the configured max rows ("
                                + maxRows
                                + ") - aborting to protect from scanning large tables.");
            }
            batch.add(currentIterator.next());
            emittedRows++;
            // Keep batch size reasonable by breaking when we collected some rows
            if (batch.size() >= 1024) {
                break;
            }
        }
        return batch.isEmpty() ? CloseableIterator.emptyIterator() : CloseableIterator.wrap(batch.iterator());
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        if (currentIterator != null) {
            try {
                currentIterator.close();
            } catch (Exception e) {
                // ignore, best effort close
            } finally {
                currentIterator = null;
            }
        }
        for (Iterator<KvSnapshotBatchScanner> it = scanners.iterator(); it.hasNext(); ) {
            KvSnapshotBatchScanner s = it.next();
            s.close();
            it.remove();
        }
    }
}
