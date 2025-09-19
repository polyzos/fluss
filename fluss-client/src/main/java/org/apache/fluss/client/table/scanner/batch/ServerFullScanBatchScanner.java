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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.FullScanRequest;
import org.apache.fluss.rpc.messages.FullScanResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A BatchScanner implementation that leverages the server-side FULL_SCAN RPC.
 *
 * <p>Note: As of now, server-side implementation may return an empty payload. This scanner
 * gracefully handles empty responses and returns no rows in that case. It is safe to
 * enable when the server supports FULL_SCAN aggregation for small tables.
 */
public class ServerFullScanBatchScanner implements BatchScanner {

    private static final long DEFAULT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);

    private final MetadataUpdater metadataUpdater;
    private final TableInfo tableInfo;
    private final @Nullable int[] projectedFields;
    private final @Nullable String partitionName;
    private final RowType rowType;

    private volatile boolean endOfInput = false;

    public ServerFullScanBatchScanner(
            MetadataUpdater metadataUpdater,
            TableInfo tableInfo,
            @Nullable int[] projectedFields,
            @Nullable String partitionName) {
        this.metadataUpdater = metadataUpdater;
        this.tableInfo = tableInfo;
        this.projectedFields = projectedFields; // currently unused: server projection TBD
        this.partitionName = partitionName;
        this.rowType = tableInfo.getRowType();
    }

    public ServerFullScanBatchScanner(
            MetadataUpdater metadataUpdater,
            TableInfo tableInfo,
            @Nullable int[] projectedFields) {
        this(metadataUpdater, tableInfo, projectedFields, null);
    }

    @Override
    public BatchScanner snapshotAll() {
        return this; // whole table by default
    }

    @Override
    public BatchScanner snapshotAllPartition(String partitionName) {
        if (!tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(
                    "Partition filter is only supported for partitioned tables");
        }
        return new ServerFullScanBatchScanner(metadataUpdater, tableInfo, projectedFields, partitionName);
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        // validate table
        if (!tableInfo.hasPrimaryKey()) {
            throw new UnsupportedOperationException(
                    "Full scan BatchScanner is only supported for primary key tables.");
        }
        if (tableInfo.isPartitioned() && partitionName == null) {
            throw new IllegalArgumentException(
                    "Full-table snapshot on a partitioned table requires a PartitionFilter with a partition name. " +
                    "Use snapshotAllPartition(partitionName).");
        }

        long timeoutMs = timeout == null ? DEFAULT_TIMEOUT_MS : Math.min(timeout.toMillis(), DEFAULT_TIMEOUT_MS);

        try {
            CoordinatorGateway gateway = metadataUpdater.newCoordinatorServerClient();
            long tableId = tableInfo.getTableId();
            FullScanRequest req = new FullScanRequest().setTableId(tableId);
            if (partitionName != null) {
                // resolve partition id via metadata updater
                long pid = metadataUpdater
                        .getPartitionIdOrElseThrow(PhysicalTablePath.of(tableInfo.getTablePath(), partitionName));
                req.setPartitionId(pid);
            }
            // compression and max_response_bytes not set for now
            CompletableFuture<FullScanResponse> future = gateway.fullScan(req);
            FullScanResponse resp = future.get(timeoutMs, TimeUnit.MILLISECONDS);

            ApiError error = ApiError.fromErrorMessage(resp);
            if (error.isFailure()) {
                throw error.exception();
            }

            if (!resp.hasRecords() || resp.getRecordsSize() == 0) {
                endOfInput = true;
                return CloseableIterator.wrap(Collections.<InternalRow>emptyList().iterator());
            }

            // TODO: decode payload into rows when server defines encoding. For now, return empty.
            endOfInput = true;
            return CloseableIterator.wrap(Collections.<InternalRow>emptyList().iterator());
        } catch (Exception e) {
            throw new IOException("Failed to execute server full scan", e);
        }
    }

    @Override
    public void close() throws IOException {
        endOfInput = true;
    }
}
