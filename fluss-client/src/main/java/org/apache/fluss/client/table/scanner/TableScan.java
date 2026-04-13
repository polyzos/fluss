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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.batch.CompositeBatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvBatchScanner;
import org.apache.fluss.client.table.scanner.batch.KvSnapshotBatchScanner;
import org.apache.fluss.client.table.scanner.batch.LimitBatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.LogScannerImpl;
import org.apache.fluss.client.table.scanner.log.TypedLogScanner;
import org.apache.fluss.client.table.scanner.log.TypedLogScannerImpl;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** API for configuring and creating {@link LogScanner} and {@link BatchScanner}. */
public class TableScan implements Scan {
    private final FlussConnection conn;
    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;

    /** The projected fields to do projection. No projection if is null. */
    @Nullable private final int[] projectedColumns;

    /** The limited row number to read. No limit if is null. */
    @Nullable private final Integer limit;

    /** The record batch filter to apply. No filter if is null. */
    @Nullable private final Predicate recordBatchFilter;

    public TableScan(FlussConnection conn, TableInfo tableInfo, SchemaGetter schemaGetter) {
        this(conn, tableInfo, schemaGetter, null, null, null);
    }

    private TableScan(
            FlussConnection conn,
            TableInfo tableInfo,
            SchemaGetter schemaGetter,
            @Nullable int[] projectedColumns,
            @Nullable Integer limit,
            @Nullable Predicate recordBatchFilter) {
        this.conn = conn;
        this.tableInfo = tableInfo;
        this.projectedColumns = projectedColumns;
        this.limit = limit;
        this.schemaGetter = schemaGetter;
        this.recordBatchFilter = recordBatchFilter;
    }

    @Override
    public Scan project(@Nullable int[] projectedColumns) {
        return new TableScan(
                conn, tableInfo, schemaGetter, projectedColumns, limit, recordBatchFilter);
    }

    @Override
    public Scan project(List<String> projectedColumnNames) {
        int[] columnIndexes = new int[projectedColumnNames.size()];
        RowType rowType = tableInfo.getRowType();
        for (int i = 0; i < projectedColumnNames.size(); i++) {
            int index = rowType.getFieldIndex(projectedColumnNames.get(i));
            if (index < 0) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' not found in table schema. Available fields: %s, Table: %s",
                                projectedColumnNames.get(i),
                                rowType.getFieldNames(),
                                tableInfo.getTablePath()));
            }
            columnIndexes[i] = index;
        }
        return new TableScan(
                conn, tableInfo, schemaGetter, columnIndexes, limit, recordBatchFilter);
    }

    @Override
    public Scan limit(int rowNumber) {
        return new TableScan(
                conn, tableInfo, schemaGetter, projectedColumns, rowNumber, recordBatchFilter);
    }

    @Override
    public Scan filter(@Nullable Predicate predicate) {
        return new TableScan(conn, tableInfo, schemaGetter, projectedColumns, limit, predicate);
    }

    @Override
    public LogScanner createLogScanner() {
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "LogScanner doesn't support limit pushdown. Table: %s, requested limit: %d",
                            tableInfo.getTablePath(), limit));
        }

        return new LogScannerImpl(
                conn.getConfiguration(),
                tableInfo,
                conn.getMetadataUpdater(),
                conn.getClientMetricGroup(),
                conn.getOrCreateRemoteFileDownloader(),
                projectedColumns,
                schemaGetter,
                recordBatchFilter);
    }

    @Override
    public <T> TypedLogScanner<T> createTypedLogScanner(Class<T> pojoClass) {
        LogScanner base = createLogScanner();
        return new TypedLogScannerImpl<>(base, pojoClass, tableInfo, projectedColumns);
    }

    /** Returns the configured KV scanner batch size in bytes. */
    private int kvBatchSizeBytes() {
        return (int)
                conn.getConfiguration()
                        .get(ConfigOptions.CLIENT_SCANNER_KV_FETCH_MAX_BYTES)
                        .getBytes();
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket) {
        if (recordBatchFilter != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "BatchScanner doesn't support filter pushdown. Table: %s, bucket: %s",
                            tableInfo.getTablePath(), tableBucket));
        }
        if (tableInfo.hasPrimaryKey()) {
            if (limit != null) {
                return new LimitBatchScanner(
                        tableInfo,
                        tableBucket,
                        schemaGetter,
                        conn.getMetadataUpdater(),
                        projectedColumns,
                        limit);
            }
            return new KvBatchScanner(
                    tableInfo,
                    tableBucket,
                    schemaGetter,
                    conn.getMetadataUpdater(),
                    kvBatchSizeBytes(),
                    projectedColumns);
        }
        if (limit == null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Currently, for log table, BatchScanner is only available when limit is set. Table: %s, bucket: %s",
                            tableInfo.getTablePath(), tableBucket));
        }
        return new LimitBatchScanner(
                tableInfo,
                tableBucket,
                schemaGetter,
                conn.getMetadataUpdater(),
                projectedColumns,
                limit);
    }

    @Override
    public BatchScanner createBatchScanner() {
        if (!tableInfo.hasPrimaryKey()) {
            throw new UnsupportedOperationException(
                    "createBatchScanner() is only supported for Primary Key Tables. Table: "
                            + tableInfo.getTablePath());
        }
        long tableId = tableInfo.getTableId();
        int numBuckets = tableInfo.getNumBuckets();
        List<TableBucket> buckets = new ArrayList<>();
        if (!tableInfo.isPartitioned()) {
            for (int b = 0; b < numBuckets; b++) {
                buckets.add(new TableBucket(tableId, b));
            }
        } else {
            try {
                List<PartitionInfo> partitions =
                        conn.getAdmin()
                                .listPartitionInfos(tableInfo.getTablePath())
                                .get(30, TimeUnit.SECONDS);
                for (PartitionInfo partition : partitions) {
                    for (int b = 0; b < numBuckets; b++) {
                        buckets.add(new TableBucket(tableId, partition.getPartitionId(), b));
                    }
                }
            } catch (Exception e) {
                throw new FlussRuntimeException(
                        "Failed to list partitions for table " + tableInfo.getTablePath(), e);
            }
        }
        List<BatchScanner> scanners = new ArrayList<>();
        for (TableBucket bucket : buckets) {
            scanners.add(createBatchScanner(bucket));
        }
        return new CompositeBatchScanner(scanners, limit);
    }

    @Override
    public BatchScanner createBatchScanner(TableBucket tableBucket, long snapshotId) {
        if (recordBatchFilter != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "SnapshotBatchScanner doesn't support filter pushdown. Table: %s, bucket: %s, snapshot ID: %d",
                            tableInfo.getTablePath(), tableBucket, snapshotId));
        }
        if (limit != null) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Currently, SnapshotBatchScanner doesn't support limit pushdown. Table: %s, bucket: %s, snapshot ID: %d, requested limit: %d",
                            tableInfo.getTablePath(), tableBucket, snapshotId, limit));
        }
        String scannerTmpDir =
                conn.getConfiguration().getString(ConfigOptions.CLIENT_SCANNER_IO_TMP_DIR);
        Admin admin = conn.getAdmin();
        final KvSnapshotMetadata snapshotMeta;
        try {
            snapshotMeta = admin.getKvSnapshotMetadata(tableBucket, snapshotId).get();
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to get snapshot metadata for table bucket %s, snapshot ID: %d, Table: %s",
                            tableBucket, snapshotId, tableInfo.getTablePath()),
                    e);
        }

        return new KvSnapshotBatchScanner(
                tableInfo.getSchemaId(),
                tableInfo.getSchema(),
                schemaGetter,
                tableBucket,
                snapshotMeta.getSnapshotFiles(),
                projectedColumns,
                scannerTmpDir,
                tableInfo.getTableConfig().getKvFormat(),
                conn.getOrCreateRemoteFileDownloader());
    }
}
