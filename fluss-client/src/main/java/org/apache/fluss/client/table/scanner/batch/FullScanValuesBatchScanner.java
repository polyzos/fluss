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
import org.apache.fluss.rpc.messages.FullScanValuesRequest;
import org.apache.fluss.rpc.messages.FullScanValuesResponse;
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
 * Batch scanner that performs a full KV scan (values only) for a single table bucket.
 *
 * <p>This uses the server-side full-scan RPC and decodes the returned DefaultValueRecordBatch
 * into InternalRow records. Projection is applied client-side.
 */
public class FullScanValuesBatchScanner implements BatchScanner {

    private final TableInfo tableInfo;
    private final TableBucket tableBucket;
    private final MetadataUpdater metadataUpdater;
    @Nullable private final int[] projectedFields;

    private final ValueDecoder kvValueDecoder;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final CompletableFuture<FullScanValuesResponse> scanFuture;

    private boolean endOfInput;

    public FullScanValuesBatchScanner(
            TableInfo tableInfo,
            TableBucket tableBucket,
            MetadataUpdater metadataUpdater,
            @Nullable int[] projectedFields) {
        this.tableInfo = tableInfo;
        this.tableBucket = tableBucket;
        this.metadataUpdater = metadataUpdater;
        this.projectedFields = projectedFields;

        RowType rowType = tableInfo.getRowType();
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }

        // prepare request and gateway
        FullScanValuesRequest request =
                new FullScanValuesRequest()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket());
        if (tableBucket.getPartitionId() != null) {
            request.setPartitionId(tableBucket.getPartitionId());
            // ensure metadata cache has this bucket mapping
            metadataUpdater.checkAndUpdateMetadata(tableInfo.getTablePath(), tableBucket);
        }
        int leader = metadataUpdater.leaderFor(tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        if (gateway == null) {
            throw new LeaderNotAvailableException(
                    "Server " + leader + " is not found in metadata cache.");
        }
        this.scanFuture = gateway.fullScanValues(request);

        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableInfo.getTableConfig().getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));
        this.endOfInput = false;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (endOfInput) {
            return null;
        }
        try {
            FullScanValuesResponse response = scanFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            List<InternalRow> rows = parseResponse(response);
            endOfInput = true;
            return CloseableIterator.wrap(rows.iterator());
        } catch (TimeoutException e) {
            // poll next time
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private List<InternalRow> parseResponse(FullScanValuesResponse response) {
        if (!response.hasRecords()) {
            return Collections.emptyList();
        }
        ByteBuffer buffer = ByteBuffer.wrap(response.getRecords());
        DefaultValueRecordBatch valueRecords = DefaultValueRecordBatch.pointToByteBuffer(buffer);
        ValueRecordReadContext readContext = new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
        List<InternalRow> out = new ArrayList<>(valueRecords.getRecordCount());
        for (ValueRecord record : valueRecords.records(readContext)) {
            out.add(maybeProject(record.getRow()));
        }
        return out;
    }

    private InternalRow maybeProject(InternalRow originRow) {
        // Deep copy row to detach from underlying buffer and then project
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow projectedRow = ProjectedRow.from(projectedFields);
            projectedRow.replaceRow(newRow);
            return projectedRow;
        } else {
            return newRow;
        }
    }

    @Override
    public void close() throws IOException {
        scanFuture.cancel(true);
    }
}
