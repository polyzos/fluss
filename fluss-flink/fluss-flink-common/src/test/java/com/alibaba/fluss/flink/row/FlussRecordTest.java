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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlussRecordTest {

    @Test
    public void testFlussRecordCreation() {
        // Create a Row
        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        // Create a FlussRecord
        FlussRecord flussRecord =
                new FlussRecord(0, System.currentTimeMillis(), ChangeType.INSERT, row);

        // Verify the FlussRecord properties
        assertThat(flussRecord.getOffset()).isEqualTo(0);
        assertThat(flussRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(flussRecord.getRow()).isEqualTo(row);
    }

    @Test
    public void testFlussRecordEqualsAndHashCode() {
        // Create two identical Rows
        Row row1 = new Row(4);
        row1.setField(0, 1L);
        row1.setField(1, 100L);
        row1.setField(2, 5);
        row1.setField(3, "123 Test St");

        Row row2 = new Row(4);
        row2.setField(0, 1L);
        row2.setField(1, 100L);
        row2.setField(2, 5);
        row2.setField(3, "123 Test St");

        // Use the same timestamp for both records
        long timestamp = System.currentTimeMillis();

        // Create two identical FlussRecords
        FlussRecord flussRecord1 = new FlussRecord(0, timestamp, ChangeType.INSERT, row1);
        FlussRecord flussRecord2 = new FlussRecord(0, timestamp, ChangeType.INSERT, row2);

        assertThat(flussRecord1).isEqualTo(flussRecord2);
        assertThat(flussRecord1.hashCode()).isEqualTo(flussRecord2.hashCode());
    }

    @Test
    public void testFlussRecordToString() {
        // Create a Row
        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        // Create a FlussRecord
        FlussRecord flussRecord =
                new FlussRecord(0, System.currentTimeMillis(), ChangeType.INSERT, row);

        // Verify the toString method
        assertThat(flussRecord.toString()).contains("FlussRecord{");
    }
}
