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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * BatchScanner that performs a full-table (or single-partition) snapshot by reading
 * the latest KV snapshots per bucket and merging results client-side. Designed for small tables.
 */
public class DefaultBatchScanner implements BatchScanner {

    // Track underlying per-bucket scanners to ensure resources are released on early close/errors
    private final List<BatchScanner> activeBucketScanners = new ArrayList<>();

    private final TableInfo tableInfo;
    private final Admin admin;
    private final RemoteFileDownloader downloader;
    @Nullable private final int[] projectedFields;
    @Nullable private final String partitionName; // when non-null, restrict to this partition

    private final String scannerTmpDir;
    private final KvFormat kvFormat;
    private final RowType rowType;

    private volatile boolean endOfInput = false;

    public DefaultBatchScanner(
            TableInfo tableInfo,
            Admin admin,
            RemoteFileDownloader downloader,
            @Nullable int[] projectedFields,
            @Nullable String partitionName,
            String scannerTmpDir) {
        this.tableInfo = tableInfo;
        this.admin = admin;
        this.downloader = downloader;
        this.projectedFields = projectedFields;
        this.partitionName = partitionName;
        this.scannerTmpDir = scannerTmpDir;
        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.rowType = tableInfo.getRowType();
    }

    public DefaultBatchScanner(
            TableInfo tableInfo,
            Admin admin,
            RemoteFileDownloader downloader,
            @Nullable int[] projectedFields,
            String scannerTmpDir) {
        this(tableInfo, admin, downloader, projectedFields, null, scannerTmpDir);
    }

    @Override
    public BatchScanner snapshotAll() {
        // no-op: scanning whole table is the default in this implementation
        return this;
    }

    @Override
    public BatchScanner snapshotAllPartition(String partitionName) {
        if (!tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(
                    "Partition filter is only supported for partitioned tables");
        }
        return new DefaultBatchScanner(
                tableInfo, admin, downloader, projectedFields, partitionName, scannerTmpDir);
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }

        if (tableInfo.isPartitioned() && partitionName == null) {
            throw new IllegalArgumentException(
                    "Full-table snapshot on a partitioned table requires a PartitionFilter with a partition name. "
                            + "Use snapshotAllPartition(partitionName).");
        }
        if (!tableInfo.hasPrimaryKey()) {
            throw new UnsupportedOperationException(
                    "Full scan BatchScanner is only supported for primary key tables.");
        }

        List<BatchScanner> bucketScanners = new ArrayList<>();
        try {
            // 1) Fetch latest KV snapshots for scope
            final KvSnapshots kvSnapshots;
            if (partitionName == null) {
                kvSnapshots = admin.getLatestKvSnapshots(tableInfo.getTablePath()).get(30, TimeUnit.SECONDS);
            } else {
                kvSnapshots = admin.getLatestKvSnapshots(tableInfo.getTablePath(), partitionName).get(30, TimeUnit.SECONDS);
            }

            // 2) Build per-bucket snapshot scanners
            for (int bucketId : kvSnapshots.getBucketIds()) {
                long snapshotId;
                if (kvSnapshots.getSnapshotId(bucketId).isPresent()) {
                    snapshotId = kvSnapshots.getSnapshotId(bucketId).getAsLong();
                } else {
                    // No snapshot for this bucket yet; skip
                    continue;
                }
                TableBucket tableBucket = new TableBucket(
                        kvSnapshots.getTableId(), kvSnapshots.getPartitionId(), bucketId);

                KvSnapshotMetadata meta = getSnapshotMetadata(tableBucket, snapshotId);
                KvSnapshotBatchScanner scanner =
                        new KvSnapshotBatchScanner(
                                rowType,
                                tableBucket,
                                meta.getSnapshotFiles(),
                                projectedFields,
                                scannerTmpDir,
                                kvFormat,
                                downloader);
                bucketScanners.add(scanner);
            }

            // make them visible to close()
            synchronized (activeBucketScanners) {
                activeBucketScanners.clear();
                activeBucketScanners.addAll(bucketScanners);
            }

            // 3) Collect all rows from bucket scanners in one shot
            List<InternalRow> rows = BatchScanUtils.collectAllRows(bucketScanners);

            // collection closes the scanners; clear the active set
            synchronized (activeBucketScanners) {
                activeBucketScanners.clear();
            }

            endOfInput = true;
            return CloseableIterator.wrap(rows.iterator());
        } catch (Exception e) {
            // close any scanners that may have been created
            for (BatchScanner scanner : bucketScanners) {
                try { scanner.close(); } catch (Exception ignore) {}
            }
            synchronized (activeBucketScanners) {
                activeBucketScanners.clear();
            }
            throw new IOException("Failed to execute full snapshot scan", e);
        }
    }

    private KvSnapshotMetadata getSnapshotMetadata(TableBucket tableBucket, long snapshotId) {
        try {
            CompletableFuture<KvSnapshotMetadata> f = admin.getKvSnapshotMetadata(tableBucket, snapshotId);
            return f.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to get snapshot metadata for " + tableBucket
                    + ", snapshotId=" + snapshotId, e);
        }
    }

    @Override
    public void close() throws IOException {
        endOfInput = true;
        // Ensure any active bucket-level scanners are closed to free resources
        synchronized (activeBucketScanners) {
            for (BatchScanner scanner : activeBucketScanners) {
                try {
                    scanner.close();
                } catch (Exception ignore) {
                    // swallow to ensure best-effort close
                }
            }
            activeBucketScanners.clear();
        }
    }
}
