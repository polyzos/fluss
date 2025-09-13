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

package org.apache.fluss.server.tablet;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FullScanEntriesResponse;
import org.apache.fluss.rpc.messages.FullScanValuesResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFullScanEntriesRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFullScanValuesRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase covering full KV scans through TabletService RPCs: values-only and key-value entries.
 */
public class TabletServiceFullScanITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void testFullScanValuesRpc() throws Exception {
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // put one batch of kv data
        DefaultKvRecordBatch kvBatch = (DefaultKvRecordBatch) genKvRecordBatch(
                org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE);
        gateway.putKv(newPutKvRequest(tableId, 0, 1, kvBatch)).get();

        // expected current state of PK table after compaction in DATA_1_WITH_KEY_AND_VALUE
        DefaultValueRecordBatch.Builder expected = DefaultValueRecordBatch.builder();
        expected.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[]{1, "a1"}));
        expected.append(DEFAULT_SCHEMA_ID, compactedRow(DATA1_ROW_TYPE, new Object[]{2, "b1"}));

        // invoke full scan (values)
        FullScanValuesResponse resp = gateway.fullScanValues(newFullScanValuesRequest(tableId, 0)).get();
        assertThat(resp.hasErrorCode()).as("No error expected").isFalse();
        DefaultValueRecordBatch actual = DefaultValueRecordBatch.pointToByteBuffer(
                ByteBuffer.wrap(resp.getRecords()));
        assertThat(actual).isEqualTo(expected.build());
    }

    @Test
    void testFullScanEntriesRpc() throws Exception {
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // put one batch of kv data
        DefaultKvRecordBatch kvBatch = (DefaultKvRecordBatch) genKvRecordBatch(
                org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE);
        gateway.putKv(newPutKvRequest(tableId, 0, 1, kvBatch)).get();

        // expected keys and values
        RowType rowType = DATA1_ROW_TYPE;
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, new int[]{0});
        byte[] k1 = keyEncoder.encodeKey(org.apache.fluss.testutils.DataTestUtils.row(new Object[]{1}));
        byte[] k2 = keyEncoder.encodeKey(org.apache.fluss.testutils.DataTestUtils.row(new Object[]{2}));
        byte[] v1 = ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, compactedRow(rowType, new Object[]{1, "a1"}));
        byte[] v2 = ValueEncoder.encodeValue(DEFAULT_SCHEMA_ID, compactedRow(rowType, new Object[]{2, "b1"}));
        Map<String, byte[]> expected = new HashMap<>();
        expected.put(Arrays.toString(k1), v1);
        expected.put(Arrays.toString(k2), v2);

        // invoke full scan (entries)
        FullScanEntriesResponse resp = gateway.fullScanEntries(newFullScanEntriesRequest(tableId, 0)).get();
        assertThat(resp.hasErrorCode()).as("No error expected").isFalse();
        Map<String, byte[]> actual = new HashMap<>();
        for (int i = 0; i < resp.getEntriesCount(); i++) {
            org.apache.fluss.rpc.messages.PbBytesKeyValue e = resp.getEntryAt(i);
            actual.put(Arrays.toString(e.getKey()), e.hasValue() ? e.getValue() : null);
        }
        assertThat(actual).hasSize(2);
        // compare value bytes equality
        assertThat(actual.keySet()).containsExactlyInAnyOrderElementsOf(expected.keySet());
        for (Map.Entry<String, byte[]> en : expected.entrySet()) {
            assertThat(actual.get(en.getKey())).isEqualTo(en.getValue());
        }
    }
}
