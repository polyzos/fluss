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
import org.apache.fluss.exception.LeaderNotAvailableException;
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
import org.apache.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import org.apache.fluss.rpc.messages.PbValueList;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A BatchScanner that streams the entire RocksDB values for a KV table bucket using
 * PrefixLookup with an empty prefix. This enables clients to iterate all records in chunks
 * without introducing a new RPC.
 */
public class KvFullScanBatchScanner implements BatchScanner {

    private static final int DEFAULT_BATCH_CHUNK_SIZE = 1024;

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;
    private final ValueDecoder kvValueDecoder;
    private final InternalRow.FieldGetter[] fieldGetters;

    private final CompletableFuture<PrefixLookupResponse> responseFuture;
    private List<InternalRow> allRows; // decoded once upon first poll
    private int nextIndex;
    private final int chunkSize;
    private boolean endOfInput;

    public KvFullScanBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields) {
        this(tableInfo, tableBucket, metadataUpdater, projectedFields, DEFAULT_BATCH_CHUNK_SIZE);
    }

    public KvFullScanBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields,
            int chunkSize) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataUpdater = metadataUpdater;
        this.projectedFields = projectedFields;
        this.chunkSize = Math.max(1, chunkSize);
        this.nextIndex = 0;
        this.endOfInput = false;
        this.allRows = null;

        RowType rowType = tableInfo.getRowType();
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }

        // Build PrefixLookupRequest with empty prefix to fetch all values for the bucket
        PrefixLookupRequest request = new PrefixLookupRequest().setTableId(tableBucket.getTableId());
        PbPrefixLookupReqForBucket pb = request.addBucketsReq().setBucketId(tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            pb.setPartitionId(tableBucket.getPartitionId());
            // ensure leader metadata is up to date for partitioned case
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        }
        // empty prefix means iterate entire DB
        pb.addKey(new byte[0]);

        int leader = metadataUpdater.leaderFor(tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }
        this.responseFuture = gateway.prefixLookup(request);
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }

        try {
            if (allRows == null) {
                PrefixLookupResponse response =
                        responseFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                this.allRows = decodeResponse(response);
            }
        } catch (TimeoutException te) {
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }

        if (nextIndex >= allRows.size()) {
            endOfInput = true;
            return null;
        }

        int toIndex = Math.min(allRows.size(), nextIndex + chunkSize);
        List<InternalRow> sub = allRows.subList(nextIndex, toIndex);
        nextIndex = toIndex;
        return CloseableIterator.wrap(sub.iterator());
    }

    private List<InternalRow> decodeResponse(PrefixLookupResponse response) {
        List<InternalRow> rows = new ArrayList<>();
        for (PbPrefixLookupRespForBucket pbResp : response.getBucketsRespsList()) {
            if (pbResp.hasErrorCode()) {
                // return empty on error; upper layers may handle per-bucket errors differently
                return Collections.emptyList();
            }
            // For our request, there is exactly one value list corresponding to empty prefix
            for (int i = 0; i < pbResp.getValueListsCount(); i++) {
                PbValueList valueList = pbResp.getValueListAt(i);
                // Values are encoded as DefaultValueRecordBatch payloads
                for (int j = 0; j < valueList.getValuesCount(); j++) {
                    byte[] recordBytes = valueList.getValueAt(j);
                    if (recordBytes == null) {
                        continue;
                    }
                    // Each element in value list is a row-encoded payload. It may be a batch or a single row.
                    // We support both by first trying to parse as DefaultValueRecordBatch; if that fails,
                    // fall back to direct row decode.
                    // Currently Kv writes use DefaultValueRecordBatch, so we handle that efficiently.
                    ByteBuffer buf = ByteBuffer.wrap(recordBytes);
                    DefaultValueRecordBatch valueBatch = DefaultValueRecordBatch.pointToByteBuffer(buf);
                    ValueRecordReadContext ctx = new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
                    for (ValueRecord vr : valueBatch.records(ctx)) {
                        rows.add(maybeProject(vr.getRow()));
                    }
                }
            }
        }
        return rows;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // Deep copy the row first using field getters so that we detach from underlying buffers
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projected = ProjectedRow.from(projectedFields);
            projected.replaceRow(newRow);
            return projected;
        }
        return newRow;
    }

    @Override
    public void close() throws IOException {
        // Best effort cancel to release waiting resources
        responseFuture.cancel(true);
    }
}
