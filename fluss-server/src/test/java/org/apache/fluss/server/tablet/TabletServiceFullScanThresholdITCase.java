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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FullScanEntriesResponse;
import org.apache.fluss.rpc.messages.FullScanValuesResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFullScanEntriesRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFullScanValuesRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase verifying full-scan threshold enforcement over RPC. */
public class TabletServiceFullScanThresholdITCase {

    private static final Configuration STRICT_CONF = new Configuration();
    static {
        // Restrict full-scan to 1 entry to trigger threshold logic when >1 rows exist
        STRICT_CONF.set(ConfigOptions.KV_FULLSCAN_MAX_KEYS, 1);
    }

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).setClusterConf(STRICT_CONF).build();

    @Test
    void testFullScanValuesThresholdError() throws Exception {
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        DefaultKvRecordBatch kvBatch = (DefaultKvRecordBatch) genKvRecordBatch(
                org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE);
        gateway.putKv(newPutKvRequest(tableId, 0, 1, kvBatch)).get();

        FullScanValuesResponse resp = gateway.fullScanValues(newFullScanValuesRequest(tableId, 0)).get();
        assertThat(resp.hasErrorCode()).isTrue();
        assertThat(resp.getErrorCode()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
    }

    @Test
    void testFullScanEntriesThresholdError() throws Exception {
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway gateway = FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        DefaultKvRecordBatch kvBatch = (DefaultKvRecordBatch) genKvRecordBatch(
                org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE);
        gateway.putKv(newPutKvRequest(tableId, 0, 1, kvBatch)).get();

        FullScanEntriesResponse resp = gateway.fullScanEntries(newFullScanEntriesRequest(tableId, 0)).get();
        assertThat(resp.hasErrorCode()).isTrue();
        assertThat(resp.getErrorCode()).isEqualTo(Errors.UNKNOWN_SERVER_ERROR.code());
    }
}
