/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.Objects;

/** one scan record. */
// TODO: replace this with GenericRecord in the future
@Internal
public class ScanRecord<T> implements LogRecord {
    private static final long INVALID = -1L;

    private final long offset;
    private final long timestamp;
    private final ChangeType changeType;
    private final T row;
    private final @Nullable RowSerializer<T> converter;

    public ScanRecord(T row) {
        this(INVALID, INVALID, ChangeType.INSERT, row);
    }

    public ScanRecord(long offset, long timestamp, ChangeType changeType, T row) {
        this(offset, timestamp, changeType, row, null);
    }

    @SuppressWarnings("unchecked")
    private ScanRecord(
            long offset,
            long timestamp,
            ChangeType changeType,
            T row,
            @Nullable RowSerializer<T> converter) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.row = row;
        this.converter = converter;
    }

    /**
     * Creates a ScanRecord with an InternalRow that can be converted to a POJO.
     *
     * @param offset The offset of the record
     * @param timestamp The timestamp of the record
     * @param changeType The change type of the record
     * @param row The InternalRow
     * @param converter The converter to use for converting InternalRow to POJO
     * @param <P> The POJO type
     * @return A new ScanRecord that can convert InternalRow to POJO
     */
    @SuppressWarnings("unchecked")
    public static <P> ScanRecord<P> withRowSerializer(
            long offset,
            long timestamp,
            ChangeType changeType,
            InternalRow row,
            RowSerializer<P> converter) {
        return new ScanRecord<>(offset, timestamp, changeType, (P) (Object) row, converter);
    }

    /**
     * Creates a ScanRecord with an InternalRow that can be converted to a POJO.
     *
     * @param row The InternalRow
     * @param converter The converter to use for converting InternalRow to POJO
     * @param <P> The POJO type
     * @return A new ScanRecord that can convert InternalRow to POJO
     */
    @SuppressWarnings("unchecked")
    public static <P> ScanRecord<P> withRowSerializer(InternalRow row, RowSerializer<P> converter) {
        return withRowSerializer(INVALID, INVALID, ChangeType.INSERT, row, converter);
    }

    /** The position of this record in the corresponding fluss table bucket. */
    @Override
    public long logOffset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        return changeType;
    }

    /**
     * Returns the row. If the row is an InternalRow and a converter is provided, it will be
     * converted to a POJO.
     *
     * @return The row
     */
    @SuppressWarnings("unchecked")
    @Override
    public InternalRow getRow() {
        if (row instanceof InternalRow) {
            return (InternalRow) row;
        } else {
            throw new UnsupportedOperationException(
                    "Cannot get InternalRow from a ScanRecord with a POJO. Use getObject() instead.");
        }
    }

    /**
     * Returns the object stored in this record. If the object is an InternalRow and a converter is
     * provided, it will be converted to a POJO.
     *
     * @return The object
     */
    @SuppressWarnings("unchecked")
    public T getObject() {
        if (converter != null && row instanceof InternalRow) {
            return converter.fromInternalRow((InternalRow) row);
        }
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
        ScanRecord<?> that = (ScanRecord<?>) o;
        return offset == that.offset
                && changeType == that.changeType
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, changeType, row);
    }

    @Override
    public String toString() {
        return changeType.shortString() + row.toString() + "@" + offset;
    }
}
