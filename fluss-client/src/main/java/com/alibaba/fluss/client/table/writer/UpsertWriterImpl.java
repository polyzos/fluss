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
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.RowEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** The writer to write data to the primary key table. */
class UpsertWriterImpl<T> extends AbstractTableWriter implements UpsertWriter<T> {
    private final @Nullable RowSerializer<T> rowSerializer;
    private static final UpsertResult UPSERT_SUCCESS = new UpsertResult();
    private static final DeleteResult DELETE_SUCCESS = new DeleteResult();

    private final KeyEncoder primaryKeyEncoder;
    private final @Nullable int[] targetColumns;

    // same to primaryKeyEncoder if the bucket key is the same to the primary key
    private final KeyEncoder bucketKeyEncoder;

    private final KvFormat kvFormat;
    private final RowEncoder rowEncoder;
    private final FieldGetter[] fieldGetters;

    UpsertWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            @Nullable int[] partialUpdateColumns,
            WriterClient writerClient,
            MetadataUpdater metadataUpdater) {
        this(tablePath, tableInfo, partialUpdateColumns, writerClient, metadataUpdater, null);
    }

    private UpsertWriterImpl(
            TablePath tablePath,
            TableInfo tableInfo,
            @Nullable int[] partialUpdateColumns,
            WriterClient writerClient,
            MetadataUpdater metadataUpdater,
            @Nullable RowSerializer<T> rowSerializer) {
        super(tablePath, tableInfo, metadataUpdater, writerClient);
        RowType rowType = tableInfo.getRowType();
        sanityCheck(rowType, tableInfo.getPrimaryKeys(), partialUpdateColumns);

        this.rowSerializer = rowSerializer;
        this.targetColumns = partialUpdateColumns;
        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        // encode primary key using physical primary key
        this.primaryKeyEncoder =
                KeyEncoder.of(rowType, tableInfo.getPhysicalPrimaryKeys(), lakeFormat);
        this.bucketKeyEncoder =
                tableInfo.isDefaultBucketKey()
                        ? primaryKeyEncoder
                        : KeyEncoder.of(rowType, tableInfo.getBucketKeys(), lakeFormat);

        this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        this.rowEncoder = RowEncoder.create(kvFormat, rowType);
        this.fieldGetters = InternalRow.createFieldGetters(rowType);
    }

    private static void sanityCheck(
            RowType rowType, List<String> primaryKeys, @Nullable int[] targetColumns) {
        // skip check when target columns is null
        if (targetColumns == null) {
            return;
        }
        BitSet targetColumnsSet = new BitSet();
        for (int targetColumnIndex : targetColumns) {
            targetColumnsSet.set(targetColumnIndex);
        }

        BitSet pkColumnSet = new BitSet();
        // check the target columns contains the primary key
        for (String key : primaryKeys) {
            int pkIndex = rowType.getFieldIndex(key);
            if (!targetColumnsSet.get(pkIndex)) {
                throw new IllegalArgumentException(
                        String.format(
                                "The target write columns %s must contain the primary key columns %s.",
                                rowType.project(targetColumns).getFieldNames(), primaryKeys));
            }
            pkColumnSet.set(pkIndex);
        }

        // check the columns not in targetColumns should be nullable
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            // column not in primary key
            if (!pkColumnSet.get(i)) {
                // the column should be nullable
                if (!rowType.getTypeAt(i).isNullable()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partial Update requires all columns except primary key to be nullable, but column %s is NOT NULL.",
                                    rowType.getFieldNames().get(i)));
                }
            }
        }
    }

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<UpsertResult> upsert(T row) {
        InternalRow internalRow;
        if (rowSerializer != null) {
            internalRow = rowSerializer.toInternalRow(row);
        } else if (row instanceof InternalRow) {
            internalRow = (InternalRow) row;
        } else {
            throw new IllegalArgumentException(
                    "Row is not an InternalRow and no RowConverter was provided. "
                            + "Use withRowConverter() to provide a converter for your POJO type.");
        }

        return upsertInternal(internalRow);
    }

    private CompletableFuture<UpsertResult> upsertInternal(InternalRow row) {
        checkFieldCount(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forUpsert(
                        getPhysicalPath(row), encodeRow(row), key, bucketKey, targetColumns);
        return send(record).thenApply(ignored -> UPSERT_SUCCESS);
    }

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that always returns null when complete normally.
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<DeleteResult> delete(T row) {
        InternalRow internalRow;
        if (rowSerializer != null) {
            internalRow = rowSerializer.toInternalRow(row);
        } else if (row instanceof InternalRow) {
            internalRow = (InternalRow) row;
        } else {
            throw new IllegalArgumentException(
                    "Row is not an InternalRow and no RowSerializer was provided. "
                            + "Use withRowSerializer() to provide a row serializer for your POJO type.");
        }

        return deleteInternal(internalRow);
    }

    private CompletableFuture<DeleteResult> deleteInternal(InternalRow row) {
        checkFieldCount(row);
        byte[] key = primaryKeyEncoder.encodeKey(row);
        byte[] bucketKey =
                bucketKeyEncoder == primaryKeyEncoder ? key : bucketKeyEncoder.encodeKey(row);
        WriteRecord record =
                WriteRecord.forDelete(getPhysicalPath(row), key, bucketKey, targetColumns);
        return send(record).thenApply(ignored -> DELETE_SUCCESS);
    }

    private BinaryRow encodeRow(InternalRow row) {
        if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
            return (IndexedRow) row;
        } else if (kvFormat == KvFormat.COMPACTED && row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        // encode the row to target format
        rowEncoder.startNewRow();
        for (int i = 0; i < fieldCount; i++) {
            rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
        }
        return rowEncoder.finishRow();
    }

    @Override
    public <P> UpsertWriter<P> withRowSerializer(RowSerializer<P> rowSerializer) {
        return new UpsertWriterWithSerializer<>(this, rowSerializer);
    }

    /** A wrapper class that converts POJOs to InternalRow using a RowConverter. */
    private static class UpsertWriterWithSerializer<P> implements UpsertWriter<P> {
        private final UpsertWriter<?> delegate;
        private final RowSerializer<P> rowSerializer;

        UpsertWriterWithSerializer(UpsertWriter<?> delegate, RowSerializer<P> rowSerializer) {
            this.delegate = delegate;
            this.rowSerializer = rowSerializer;
        }

        @Override
        public CompletableFuture<UpsertResult> upsert(P row) {
            if (row == null) {
                throw new IllegalArgumentException("Cannot upsert null row");
            }
            InternalRow internalRow = rowSerializer.toInternalRow(row);
            return ((UpsertWriter<InternalRow>) delegate).upsert(internalRow);
        }

        @Override
        public CompletableFuture<DeleteResult> delete(P row) {
            if (row == null) {
                throw new IllegalArgumentException("Cannot delete null row");
            }
            InternalRow internalRow = rowSerializer.toInternalRow(row);
            return ((UpsertWriter<InternalRow>) delegate).delete(internalRow);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public <T> UpsertWriter<T> withRowSerializer(RowSerializer<T> rowSerializer) {
            return new UpsertWriterWithSerializer<>(delegate, rowSerializer);
        }
    }
}
