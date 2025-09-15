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
package org.apache.fluss.client.lookup;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.TestData;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.types.Tuple2;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for PrimaryKeyLookuper FULL_SCAN snapshot methods. */
class PrimaryKeyLookuperFullScanTest {

    private static PrimaryKeyLookuper newLookuper(TableInfo tableInfo, TestingMetadataUpdater mu) {
        // LookupClient is not used by snapshotAll path; provide a minimal stub.
        org.apache.fluss.config.Configuration conf = new org.apache.fluss.config.Configuration();
        LookupClient dummy = new LookupClient(conf, mu) {
            @Override
            public CompletableFuture<byte[]> lookup(org.apache.fluss.metadata.TableBucket tableBucket, byte[] key) {
                return CompletableFuture.completedFuture(null);
            }
        };
        return new PrimaryKeyLookuper(tableInfo, mu, dummy);
    }

    @Test
    void testSnapshotAllEmptyAndAggregationAcrossServers() throws Exception {
        // non-partitioned PK table from TestData
        TableInfo ti = TestData.DATA2_TABLE_INFO; // PK table distributed by 3
        Map<TablePath, TableInfo> tables = new HashMap<>();
        tables.put(ti.getTablePath(), ti);
        TestingMetadataUpdater mu = new TestingMetadataUpdater(tables);

        // Enqueue one response per server (3 unique leaders because 3 buckets on different nodes)
        // server gateways created inside TestingMetadataUpdater are TestTabletServerGateway
        // We push 2 rows on server1 and 1 row on server2, 0 on server3
        DefaultValueRecordBatch.Builder b1 = DefaultValueRecordBatch.builder();
        b1.append(encodeValue(1, "a"));
        b1.append(encodeValue(2, "b"));
        DefaultValueRecordBatch.Builder b2 = DefaultValueRecordBatch.builder();
        b2.append(encodeValue(3, "c"));

        // Enqueue into known servers 1,2,3
        try {
            org.apache.fluss.record.DefaultValueRecordBatch batch1 = b1.build();
            ((TestTabletServerGateway) mu.newTabletServerClientForNode(1)).enqueueFullScanResponse(
                    new org.apache.fluss.rpc.messages.FullScanResponse()
                            .setRecords(batch1.getSegment(), batch1.getPosition(), batch1.sizeInBytes()));
        } catch (Exception e) { throw new RuntimeException(e); }
        try {
            org.apache.fluss.record.DefaultValueRecordBatch batch2 = b2.build();
            ((TestTabletServerGateway) mu.newTabletServerClientForNode(2)).enqueueFullScanResponse(
                    new org.apache.fluss.rpc.messages.FullScanResponse()
                            .setRecords(batch2.getSegment(), batch2.getPosition(), batch2.sizeInBytes()));
        } catch (Exception e) { throw new RuntimeException(e); }
        ((TestTabletServerGateway) mu.newTabletServerClientForNode(3)).enqueueFullScanResponse(
                new org.apache.fluss.rpc.messages.FullScanResponse()); // empty

        PrimaryKeyLookuper lookuper = newLookuper(ti, mu);
        List<InternalRow> rows = lookuper.snapshotAll().get();
        assertThat(rows).hasSize(3);
        // Content verification (unordered)
        assertThat(rows.stream().map(r -> Tuple2.of(r.getInt(0), r.getString(1))).toArray())
                .containsExactlyInAnyOrder(
                        Tuple2.of(1, "a"), Tuple2.of(2, "b"), Tuple2.of(3, "c"));
    }

    @Test
    void testSnapshotAllPartitionedTableThrows() {
        // Make a partitioned PK table descriptor
        TableDescriptor td = TableDescriptor.builder()
                .schema(TestData.DATA2_SCHEMA)
                .distributedBy(3, "a")
                .partitionedBy("b")
                .build();
        TableInfo ti = TableInfo.of(TablePath.of("db", "t"), 99L, 1, td, 1L, 1L);
        Map<TablePath, TableInfo> tables = new HashMap<>();
        tables.put(ti.getTablePath(), ti);
        TestingMetadataUpdater mu = new TestingMetadataUpdater(tables);

        PrimaryKeyLookuper lookuper = newLookuper(ti, mu);
        assertThatThrownBy(() -> lookuper.snapshotAll().join())
                .hasCauseInstanceOf(org.apache.fluss.exception.TableNotPartitionedException.class);
    }

    @Test
    void testSnapshotAllPartitionErrorPropagation() {
        // Use non-partitioned table metadata but call partition variant with unknown partition
        TableInfo ti = TestData.DATA2_TABLE_INFO;
        Map<TablePath, TableInfo> tables = new HashMap<>();
        tables.put(ti.getTablePath(), ti);
        TestingMetadataUpdater mu = new TestingMetadataUpdater(tables);
        PrimaryKeyLookuper lookuper = newLookuper(ti, mu);

        assertThatThrownBy(() -> lookuper.snapshotAllPartition("2024").join())
                .hasCauseInstanceOf(org.apache.fluss.exception.PartitionNotExistException.class);
    }

    @Test
    void testServerErrorPropagatesToClient() {
        TableInfo ti = TestData.DATA2_TABLE_INFO;
        Map<TablePath, TableInfo> tables = new HashMap<>();
        tables.put(ti.getTablePath(), ti);
        TestingMetadataUpdater mu = new TestingMetadataUpdater(tables);

        org.apache.fluss.rpc.messages.FullScanResponse err = new org.apache.fluss.rpc.messages.FullScanResponse();
        err.setErrorCode(Errors.LEADER_NOT_AVAILABLE_EXCEPTION.code());
        err.setErrorMessage("leader not available");
        ((TestTabletServerGateway) mu.newTabletServerClientForNode(1)).enqueueFullScanResponse(err);
        // default empty for others, but the error should cause failure before we parse all
        PrimaryKeyLookuper lookuper = newLookuper(ti, mu);
        assertThatThrownBy(() -> lookuper.snapshotAll().join())
                .hasCauseInstanceOf(LeaderNotAvailableException.class);
    }

    private static byte[] encodeValue(int a, String b) { 
            org.apache.fluss.metadata.KvFormat kv = TestData.DATA2_TABLE_INFO.getTableConfig().getKvFormat();
            org.apache.fluss.row.encode.RowEncoder re = org.apache.fluss.row.encode.RowEncoder.create(kv, TestData.DATA2_SCHEMA.getRowType());
            re.startNewRow();
            re.encodeField(0, a);
            re.encodeField(1, b);
            org.apache.fluss.row.BinaryRow row = re.finishRow();
            return org.apache.fluss.row.encode.ValueEncoder.encodeValue((short)1, row);
        }
}
