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

package com.alibaba.fluss.flink.source.emitter;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRecordEmitter} with RowData output type. */
public class FlinkRecordEmitterTest extends FlinkTestBase {

    private FlussDeserializationSchema<RowData> deserializationSchema;
    private SourceReaderMetricGroup metricsGroup;
    private TestSourceOutput sourceOutput;
    private FlinkRecordEmitter<RowData> emitter;

    @Test
    void testEmitRecordWithHybridSplitInSnapshotPhase() throws Exception {
        // Setup
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);

        TableBucket bucket0 = new TableBucket(tableId, 0);

        HybridSnapshotLogSplit hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(bucket0, null, 0L, 0L);

        HybridSnapshotLogSplitState splitState =
                new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);

        ScanRecord scanRecord = new ScanRecord(-1, 100L, ChangeType.INSERT, row(1, "a"));
        // Create array of data types first
        DataType[] dataTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};

        String[] fieldNames = new String[] {"id", "name"};

        RowType sourceOutputType = RowType.of(dataTypes, fieldNames);

        RecordAndPos recordAndPos = new RecordAndPos(scanRecord, 42L);

        FlussRowToFlinkRowConverter converter = new FlussRowToFlinkRowConverter(sourceOutputType);
        this.deserializationSchema = new RowDataDeserializationSchema();
        this.emitter =
                new FlinkRecordEmitter<RowData>(
                        deserializationSchema, sourceOutputType, metricsGroup);

        this.sourceOutput = new TestSourceOutput();

        // Execute
        emitter.emitRecord(recordAndPos, sourceOutput, splitState);
        List<RowData> results = this.sourceOutput.getRecords();

        ArrayList<RowData> expectedResult = new ArrayList<>();
        expectedResult.add(converter.toFlinkRowData(row(1, "a")));

        assertThat(splitState.isHybridSnapshotLogSplitState()).isTrue();
        assertThat(results).hasSize(1);
        assertThat(results).isEqualTo(expectedResult);
    }

    private static class TestSourceOutput implements SourceOutput<RowData> {
        private final List<RowData> records = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public void collect(RowData record) {
            System.out.println("collect record: " + record);
        }

        @Override
        public void collect(RowData record, long timestamp) {
            records.add(record);
        }

        public List<RowData> getRecords() {
            return this.records;
        }
    }
}
