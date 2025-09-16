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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.FullScanRequest;
import org.apache.fluss.rpc.messages.FullScanResponse;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Consolidated tests for PrimaryKeyLookuper covering snapshotAll() and snapshotAllPartition(). */
class PrimaryKeyLookuperTest {

    private TableInfo nonPartitionedPk;
    private TestingMetadataUpdater metadataUpdater;

    @BeforeEach
    void setUp() {
        nonPartitionedPk = DATA1_TABLE_INFO_PK;
        Map<TablePath, TableInfo> tableInfos = new HashMap<>();
        tableInfos.put(DATA1_TABLE_PATH_PK, nonPartitionedPk);
        metadataUpdater = new TestingMetadataUpdater(tableInfos);
    }

    @Test
    void testSnapshotAll() {
        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);
        PrimaryKeyLookuper lookuper =
                new PrimaryKeyLookuper(nonPartitionedPk, metadataUpdater, lookupClient);

        // Kick off snapshotAll which will enqueue FullScan requests to leaders (1,2,3)
        CompletableFuture<List<InternalRow>> future = lookuper.snapshotAll();

        int leader = metadataUpdater.leaderFor(new TableBucket(DATA1_TABLE_ID_PK, 0));
        System.out.println(leader);
        TestTabletServerGateway gateway =
                (TestTabletServerGateway) metadataUpdater.newTabletServerClientForNode(leader);
        FullScanRequest request = new FullScanRequest().setTableId(nonPartitionedPk.getTableId());
        System.out.println(request);
        CompletableFuture<FullScanResponse> fullScanResponseCompletableFuture =
                gateway.fullScan(request);
        fullScanResponseCompletableFuture.join();

        //        // Prepare 10 records split across 3 leaders: 4 + 3 + 3
        //        List<Object[]> all = DATA1; // already contains 10 rows matching the schema (a
        // INT, b STRING)
        //        List<Object[]> part1 = all.subList(0, 4);
        //        List<Object[]> part2 = all.subList(4, 7);
        //        List<Object[]> part3 = all.subList(7, 10);
        //
        //        respondWithRecords(1, part1);
        //        respondWithRecords(2, part2);
        //        respondWithRecords(3, part3);
        //
        //        List<InternalRow> rows = future.join();
        //        assertThat(rows).hasSize(10);
    }

    @Test
    void snapshotAllPartition_throwsOnNonPartitionedTable() {
        LookupClient lookupClient = new LookupClient(new Configuration(), metadataUpdater);
        PrimaryKeyLookuper lookuper =
                new PrimaryKeyLookuper(nonPartitionedPk, metadataUpdater, lookupClient);
        assertThatThrownBy(() -> lookuper.snapshotAllPartition("p1").join())
                .hasMessageContaining("Table is not partitioned");
    }
}
