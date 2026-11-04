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

package org.apache.fluss.lake.paimon.tiering.mergetree;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.types.RowType;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowKind;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toRowKind;

/** A {@link RecordWriter} to write to Paimon's primary-key table. */
public class MergeTreeWriter extends RecordWriter<KeyValue> {

    private final KeyValue keyValue = new KeyValue();

    private final RowKeyExtractor rowKeyExtractor;

    private final IOManager ioManager;

    @Nullable private final Roaring64Bitmap logDvBitmap;

    public MergeTreeWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            RowType flussRowType,
            @Nullable String[] ioTmpDirs,
            boolean paimonIncludingSystemColumns,
            boolean historicalPartition,
            boolean dvEnabled,
            @Nullable byte[] logDvBitmapBytes) {
        this(
                fileStoreTable,
                createIOManager(ioTmpDirs),
                tableBucket,
                partition,
                partitionKeys,
                flussRowType,
                paimonIncludingSystemColumns,
                historicalPartition,
                dvEnabled,
                logDvBitmapBytes);
    }

    MergeTreeWriter(
            FileStoreTable fileStoreTable,
            IOManager ioManager,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            RowType flussRowType,
            boolean paimonIncludingSystemColumns,
            boolean historicalPartition,
            boolean dvEnabled,
            @Nullable byte[] logDvBitmapBytes) {
        super(
                createTableWrite(fileStoreTable, ioManager),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys,
                flussRowType,
                paimonIncludingSystemColumns,
                historicalPartition,
                dvEnabled);
        this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
        this.ioManager = ioManager;
        if (logDvBitmapBytes != null) {
            this.logDvBitmap = new Roaring64Bitmap();
            try {
                this.logDvBitmap.deserialize(ByteBuffer.wrap(logDvBitmapBytes));
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize LogDv bitmap", e);
            }
        } else {
            this.logDvBitmap = null;
        }
    }

    private static IOManager createIOManager(@Nullable String[] ioTmpDirs) {
        return IOManager.create(getIoManagerTmpDirs(ioTmpDirs));
    }

    static String[] getIoManagerTmpDirs(@Nullable String[] ioTmpDirs) {
        if (ioTmpDirs != null && ioTmpDirs.length > 0) {
            return ioTmpDirs;
        }
        return new String[] {System.getProperty("java.io.tmpdir")};
    }

    private static TableWriteImpl<KeyValue> createTableWrite(
            FileStoreTable fileStoreTable, IOManager ioManager) {
        //noinspection unchecked
        return (TableWriteImpl<KeyValue>)
                fileStoreTable.newWrite(FLUSS_LAKE_TIERING_COMMIT_USER).withIOManager(ioManager);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            ioManager.close();
        }
    }

    @Override
    public void write(LogRecord record) throws Exception {
        if (dvEnabled) {
            writeDvMode(record);
        } else {
            writeNormalMode(record);
        }
    }

    private void writeNormalMode(LogRecord record) throws Exception {
        BinaryRow targetPartition = prepareRecordAndGetPartition(record);

        rowKeyExtractor.setRecord(flussRecordAsPaimonRow);
        keyValue.replace(
                rowKeyExtractor.trimmedPrimaryKey(),
                KeyValue.UNKNOWN_SEQUENCE,
                toRowKind(record.getChangeType()),
                flussRecordAsPaimonRow);
        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        tableWrite.getWrite().write(targetPartition, bucket, keyValue);
    }

    private void writeDvMode(LogRecord record) throws Exception {
        ChangeType changeType = record.getChangeType();

        // Skip -U (UPDATE_BEFORE): not written to Paimon in DV mode
        if (changeType == ChangeType.UPDATE_BEFORE) {
            return;
        }

        // For +I/+U: check LogDv filter
        if (changeType == ChangeType.INSERT || changeType == ChangeType.UPDATE_AFTER) {
            if (logDvBitmap != null && logDvBitmap.contains(record.logOffset())) {
                // This record has been superseded by a later -U/-D, skip
                return;
            }
        }

        BinaryRow targetPartition = prepareRecordAndGetPartition(record);
        rowKeyExtractor.setRecord(flussRecordAsPaimonRow);

        // Use logOffset as sequence number for DV ordering
        long seq = record.logOffset();
        RowKind rowKind;
        if (changeType == ChangeType.DELETE) {
            rowKind = RowKind.DELETE;
        } else {
            // Both +I and +U written as INSERT
            rowKind = RowKind.INSERT;
        }

        keyValue.replace(rowKeyExtractor.trimmedPrimaryKey(), seq, rowKind, flussRecordAsPaimonRow);
        tableWrite.getWrite().write(targetPartition, bucket, keyValue);
    }
}
