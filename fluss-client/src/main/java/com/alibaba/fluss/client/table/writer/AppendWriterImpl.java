/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.client.write.WriteRecord;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.row.encode.IndexedRowEncoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the log table. */
class AppendWriterImpl<T> extends AbstractTableWriter implements AppendWriter<T> {
    private static final AppendResult APPEND_SUCCESS = new AppendResult();

    private final @Nullable KeyEncoder bucketKeyEncoder;

    private final LogFormat logFormat;
    private final IndexedRowEncoder indexedRowEncoder;
    private final FieldGetter[] fieldGetters;
    private final @Nullable RowSerializer<T> rowSerializer;

    AppendWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            WriterClient writerClient) {
        this(tablePath, tableInfo, metadataUpdater, writerClient, null);
    }

    private AppendWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            WriterClient writerClient,
            @Nullable RowSerializer<T> rowSerializer) {
        super(tablePath, tableInfo, metadataUpdater, writerClient);
        List<String> bucketKeys = tableInfo.getBucketKeys();
        if (bucketKeys.isEmpty()) {
            this.bucketKeyEncoder = null;
        } else {
            RowType rowType = tableInfo.getSchema().getRowType();
            DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
            this.bucketKeyEncoder = KeyEncoder.of(rowType, bucketKeys, lakeFormat);
        }

        this.logFormat = tableInfo.getTableConfig().getLogFormat();
        this.indexedRowEncoder = new IndexedRowEncoder(tableInfo.getRowType());
        this.fieldGetters = InternalRow.createFieldGetters(tableInfo.getRowType());
        this.rowSerializer = rowSerializer;
    }

    /**
     * Append row into Fluss non-pk table.
     *
     * @param row the row to append.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<AppendResult> append(T row) {
        InternalRow internalRow;
        if (rowSerializer != null) {
            internalRow = rowSerializer.toInternalRow(row);
        } else if (row instanceof InternalRow) {
            internalRow = (InternalRow) row;
        } else {
            throw new IllegalArgumentException(
                    "Row is not an InternalRow and no RowSerializer was provided. "
                            + "Use withRowSerializer() to provide a serializer for your POJO type.");
        }

        return appendInternal(internalRow);
    }

    private CompletableFuture<AppendResult> appendInternal(InternalRow row) {
        checkFieldCount(row);

        PhysicalTablePath physicalPath = getPhysicalPath(row);
        byte[] bucketKey = bucketKeyEncoder != null ? bucketKeyEncoder.encodeKey(row) : null;

        final WriteRecord record;
        if (logFormat == LogFormat.INDEXED) {
            IndexedRow indexedRow = encodeIndexedRow(row);
            record = WriteRecord.forIndexedAppend(physicalPath, indexedRow, bucketKey);
        } else {
            // ARROW format supports general internal row
            record = WriteRecord.forArrowAppend(physicalPath, row, bucketKey);
        }
        return send(record).thenApply(r -> APPEND_SUCCESS);
    }

    @Override
    public <P> AppendWriter<P> withRowSerializer(RowSerializer<P> rowSerializer) {
        return new AppendWriterWithSerializer<>(this, rowSerializer);
    }

    /** A wrapper class that converts POJOs to InternalRow using a RowConverter. */
    private static class AppendWriterWithSerializer<P> implements AppendWriter<P> {
        private final AppendWriter<?> delegate;
        private final RowSerializer<P> converter;

        AppendWriterWithSerializer(AppendWriter<?> delegate, RowSerializer<P> converter) {
            this.delegate = delegate;
            this.converter = converter;
        }

        @Override
        public CompletableFuture<AppendResult> append(P row) {
            if (row == null) {
                throw new IllegalArgumentException("Cannot append null row");
            }
            InternalRow internalRow = converter.toInternalRow(row);
            return ((AppendWriter<InternalRow>) delegate).append(internalRow);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public <T> AppendWriter<T> withRowSerializer(RowSerializer<T> rowSerializer) {
            return new AppendWriterWithSerializer<>(delegate, rowSerializer);
        }
    }

    private IndexedRow encodeIndexedRow(InternalRow row) {
        if (row instanceof IndexedRow) {
            return (IndexedRow) row;
        }

        indexedRowEncoder.startNewRow();
        for (int i = 0; i < super.fieldCount; i++) {
            indexedRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return indexedRowEncoder.finishRow();
    }
}
