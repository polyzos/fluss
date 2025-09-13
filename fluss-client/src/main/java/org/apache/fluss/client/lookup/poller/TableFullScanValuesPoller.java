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

package org.apache.fluss.client.lookup.poller;

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.ValueRecord;
import org.apache.fluss.record.ValueRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.row.decode.RowDecoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FullScanValuesRequest;
import org.apache.fluss.rpc.messages.FullScanValuesResponse;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Default implementation of {@link FullTablePoller} that periodically issues full-scan (values-only)
 * RPCs to each bucket leader and merges results into a single snapshot.
 */
public class TableFullScanValuesPoller implements FullTablePoller {

    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    private final Duration period;
    @Nullable private final int[] projectedFields;

    private final InternalRow.FieldGetter[] fieldGetters;
    private final ValueDecoder kvValueDecoder;

    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> scheduled;

    private final List<Consumer<List<InternalRow>>> listeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Throwable>> errorListeners = new CopyOnWriteArrayList<>();

    @Nullable private volatile List<InternalRow> latest;

    public TableFullScanValuesPoller(
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            Duration period,
            @Nullable int[] projectedFields) {
        this.tableInfo = Objects.requireNonNull(tableInfo, "tableInfo");
        this.metadataUpdater = Objects.requireNonNull(metadataUpdater, "metadataUpdater");
        this.period = Objects.requireNonNull(period, "period");
        this.projectedFields = projectedFields;

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
        this.kvValueDecoder = new ValueDecoder(
                RowDecoder.create(
                        tableInfo.getTableConfig().getKvFormat(),
                        rowType.getChildren().toArray(new DataType[0])));

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("fluss-fulltable-poller");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        if (scheduled != null && !scheduled.isCancelled()) {
            return;
        }
        scheduled = scheduler.scheduleWithFixedDelay(this::pollOnceSafe, 0L, period.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        ScheduledFuture<?> s = this.scheduled;
        if (s != null) {
            s.cancel(false);
            this.scheduled = null;
        }
    }

    @Override
    public void subscribe(Consumer<List<InternalRow>> listener) {
        listeners.add(listener);
    }

    @Override
    public void subscribeErrors(Consumer<Throwable> errorListener) {
        errorListeners.add(errorListener);
    }

    @Override
    public @Nullable List<InternalRow> latest() {
        return latest;
    }

    @Override
    public Duration period() {
        return period;
    }

    @Override
    public void close() {
        stop();
        scheduler.shutdownNow();
    }

    private void pollOnceSafe() {
        try {
            List<InternalRow> rows = fetchAllBucketsOnce();
            latest = rows;
            for (Consumer<List<InternalRow>> l : listeners) {
                try { l.accept(rows); } catch (Throwable ignored) {}
            }
        } catch (Throwable t) {
            for (Consumer<Throwable> l : errorListeners) {
                try { l.accept(t); } catch (Throwable ignored) {}
            }
        }
    }

    private List<InternalRow> fetchAllBucketsOnce() throws IOException {
        int numBuckets = tableInfo.getNumBuckets();
        List<InternalRow> out = new ArrayList<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            TableBucket tb = new TableBucket(tableInfo.getTableId(), bucket);
            // Ensure leader mapping if partitioned later
            if (tb.getPartitionId() != null) {
                metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tb);
            }
            int leader = metadataUpdater.leaderFor(tb);
            TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
            if (gateway == null) {
                throw new IOException("Leader gateway not found for node: " + leader);
            }
            FullScanValuesRequest req = new FullScanValuesRequest()
                    .setTableId(tb.getTableId())
                    .setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                req.setPartitionId(tb.getPartitionId());
            }
            CompletableFuture<FullScanValuesResponse> fut = gateway.fullScanValues(req);
            FullScanValuesResponse resp;
            try {
                resp = fut.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IOException("fullScanValues RPC failed", e);
            }
            out.addAll(parseResponse(resp));
        }
        return out;
    }

    private List<InternalRow> parseResponse(FullScanValuesResponse response) throws IOException {
        if (response.hasErrorCode()) {
            throw new IOException("FullScanValues error: code=" + response.getErrorCode()
                    + ", message=" + (response.hasErrorMessage() ? response.getErrorMessage() : ""));
        }
        if (!response.hasRecords()) {
            return Collections.emptyList();
        }
        ByteBuffer buffer = ByteBuffer.wrap(response.getRecords());
        DefaultValueRecordBatch valueRecords = DefaultValueRecordBatch.pointToByteBuffer(buffer);
        ValueRecordReadContext readContext = new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
        List<InternalRow> rows = new ArrayList<>(valueRecords.getRecordCount());
        for (ValueRecord record : valueRecords.records(readContext)) {
            rows.add(maybeProject(record.getRow()));
        }
        return rows;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // Deep copy row then project optionally
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
            projectedRow.replaceRow(newRow);
            return projectedRow;
        }
        return newRow;
    }
}
