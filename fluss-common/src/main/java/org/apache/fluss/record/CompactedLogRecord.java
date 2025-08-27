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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.record.DefaultLogRecordBatch.LENGTH_LENGTH;

/**
 * Log record for CompactedRow-based log format. Layout is identical to IndexedLogRecord:
 * [Length:int32][Attributes:int8][Value:CompactedRow bytes].
 */
@PublicEvolving
public class CompactedLogRecord implements LogRecord {

    private static final int ATTRIBUTES_LENGTH = 1;

    private final long logOffset;
    private final long timestamp;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    private CompactedLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.timestamp = timestamp;
        this.fieldTypes = fieldTypes;
    }

    private void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        byte attributes = segment.get(offset + LENGTH_LENGTH);
        return ChangeType.fromByteValue(attributes);
    }

    @Override
    public InternalRow getRow() {
        int rowOffset = LENGTH_LENGTH + ATTRIBUTES_LENGTH;
        int rowLength = sizeInBytes - rowOffset;
        // construct a CompactedRow backed by the record bytes
        CompactedRow row =
                new CompactedRow(fieldTypes.length, new CompactedRowDeserializer(fieldTypes));
        row.pointTo(segment, offset + rowOffset, rowLength);
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactedLogRecord that = (CompactedLogRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    /** Write the record to output and return record bytes including the leading length. */
    public static int writeTo(OutputView outputView, ChangeType changeType, CompactedRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);
        outputView.writeInt(sizeInBytes);
        outputView.writeByte(changeType.toByteValue());
        CompactedRowWriter.serializeCompactedRow(row, outputView);
        return sizeInBytes + LENGTH_LENGTH;
    }

    public static CompactedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        int sizeInBytes = segment.getInt(position);
        CompactedLogRecord rec = new CompactedLogRecord(logOffset, logTimestamp, colTypes);
        rec.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        return rec;
    }

    public static int sizeOf(CompactedRow row) {
        return calculateSizeInBytes(row) + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(CompactedRow row) {
        int size = 1; // attributes
        size += row.getSizeInBytes();
        return size;
    }
}
