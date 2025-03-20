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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.record.ChangeType;

import org.apache.flink.types.Row;

import java.util.Objects;

public class FlussRecord {
    private final long offset;
    private final long timestamp;
    private final ChangeType changeType;
    private final Row row;

    public FlussRecord(long offset, long timestamp, ChangeType changeType, Row row) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.changeType = changeType;
        this.row = row;
    }

    /** The position of this record in the corresponding fluss table bucket. */
    public long getOffset() {
        return offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public Row getRow() {
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussRecord that = (FlussRecord) o;
        return offset == that.offset
                && timestamp == that.timestamp
                && changeType == that.changeType
                && Objects.equals(row, that.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, timestamp, changeType, row);
    }

    @Override
    public String toString() {
        return "FlussRecord{"
                + "offset="
                + offset
                + ", timestamp="
                + timestamp
                + ", changeType="
                + changeType
                + ", row="
                + row
                + '}';
    }
}
