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
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.row.encode.CompactedKeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.NewScanRequestPB;
import org.apache.fluss.rpc.messages.ScanRequest;
import org.apache.fluss.rpc.messages.ScanResponse;
import org.apache.fluss.rpc.messages.ScannerKeepAliveRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DataField;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for KV full scan API (kvScan). */
public class TabletServiceKvScanITCase {

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static ScanRequest newOpenScanReq(int bucketId, Integer partitionId, Integer batchSizeBytes) {
        ScanRequest req = new ScanRequest();
        NewScanRequestPB ns = new NewScanRequestPB();
        if (partitionId != null) ns.setPartitionId(partitionId);
        ns.setBucketId(bucketId);
        req.setNewScanRequest(ns);
        if (batchSizeBytes != null) req.setBatchSizeBytes(batchSizeBytes);
        return req;
    }

    private static ScanRequest nextPageReq(byte[] scannerId, Integer batchSizeBytes, boolean close) {
        ScanRequest req = new ScanRequest().setScannerId(scannerId);
        if (batchSizeBytes != null) req.setBatchSizeBytes(batchSizeBytes);
        if (close) req.setCloseScanner(true);
        return req;
    }

    private static long countRecordsInBatch(DefaultValueRecordBatch batch) {
        return batch.getRecordCount();
    }

    @Test
    @Timeout(120)
    public void testKvScanPaginationSingleBucket() throws Exception {
        long tableId = createTable(CLUSTER, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        CLUSTER.waitUntilAllReplicaReady(tb);
        int leader = CLUSTER.waitAndGetLeader(tb);
        TabletServerGateway gw = CLUSTER.newTabletServerClientForNode(leader);

        // seed 10_000 kvs with tiny values
        seedKv(gw, tableId, 0, 10_000, 100);

        // open scanner with small batch size to force pagination
        ScanResponse open = gw.scan(newOpenScanReq(0, null, 4 * 1024)).get();
        assertThat(open.hasErrorCode()).isFalse();
        assertThat(open.hasScannerId()).isTrue();
        byte[] scannerId = open.getScannerId();
        long total = 0;
        if (open.hasRecords()) {
            DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(open.getRecords());
            total += countRecordsInBatch(batch);
        }
        boolean more = open.isHasMoreResults();
        while (more) {
            ScanResponse page = gw.scan(nextPageReq(scannerId, 4 * 1024, false)).get();
            assertThat(page.hasErrorCode()).isFalse();
            if (page.hasRecords()) {
                DefaultValueRecordBatch batch = DefaultValueRecordBatch.pointToBytes(page.getRecords());
                total += countRecordsInBatch(batch);
            }
            more = page.isHasMoreResults();
        }
        assertThat(total).isEqualTo(10_000);
    }

    @Test
    @Timeout(180)
    public void testKvScanAcrossThreeBuckets() throws Exception {
        // Create a table distributed by 3 buckets
        long tableId = createTable(
                CLUSTER,
                TablePath.of("db_scan", "tbl_three_buckets"),
                TableDescriptor.builder()
                        .schema(DATA1_TABLE_DESCRIPTOR_PK.getSchema())
                        .distributedBy(3, "a")
                        .build());
        // seed each bucket with 5_000 rows
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            CLUSTER.waitUntilAllReplicaReady(tb);
            int leader = CLUSTER.waitAndGetLeader(tb);
            TabletServerGateway gw = CLUSTER.newTabletServerClientForNode(leader);
            seedKv(gw, tableId, bucket, 5_000, 50);

            ScanResponse open = gw.scan(newOpenScanReq(bucket, null, 8 * 1024)).get();
            assertThat(open.hasErrorCode()).isFalse();
            long cnt = 0;
            if (open.hasRecords()) {
                cnt += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(open.getRecords()));
            }
            boolean more = open.isHasMoreResults();
            byte[] scannerId = open.getScannerId();
            while (more) {
                ScanResponse page = gw.scan(nextPageReq(scannerId, 8 * 1024, false)).get();
                if (page.hasRecords()) {
                    cnt += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(page.getRecords()));
                }
                more = page.isHasMoreResults();
            }
            assertThat(cnt).isEqualTo(5_000);
        }
    }

    @Test
    @Timeout(60)
    public void testKvScanWithNonExistingPartitionReturnsError() throws Exception {
        long tableId = createTable(CLUSTER, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        CLUSTER.waitUntilAllReplicaReady(tb);
        int leader = CLUSTER.waitAndGetLeader(tb);
        TabletServerGateway gw = CLUSTER.newTabletServerClientForNode(leader);

        // send one small batch so the tablet has kv
        seedKv(gw, tableId, 0, 10, 10);

        // request with a non-existent partition id should return error
        ScanRequest req = newOpenScanReq(0, 9999, 4 * 1024);
        ScanResponse resp = gw.scan(req).get();
        assertThat(resp.hasErrorCode()).isTrue();
        assertThat(resp.getErrorCode()).isEqualTo(Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION.code());
    }

    @Test
    @Timeout(180)
    public void testKvScanWithPartitionsAndBuckets() throws Exception {
        // Define a partitioned PK table with composite PK (a,b): partitioned by 'a' and bucketed by 'b'
        RowType rowType = DataTypes.ROW(new DataField("a", DataTypes.INT()), new DataField("b", DataTypes.STRING()));
        RowType pkType = DataTypes.ROW(new DataField("a", DataTypes.INT()), new DataField("b", DataTypes.STRING()));
        TablePath tablePath = TablePath.of("db_scan", "tbl_partitions");
        TableDescriptor desc = TableDescriptor.builder()
                .schema(org.apache.fluss.metadata.Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build())
                .partitionedBy("a")
                .distributedBy(3, "b")
                .build();
        long tableId = createTable(CLUSTER, tablePath, desc);

        // Create one logical partition for testing and get its partitionId
        java.util.Map<String, String> specMap = new java.util.HashMap<>();
        specMap.put("a", "1");
        long partitionId = org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition(
                CLUSTER, tablePath, new PartitionSpec(specMap), true);

        // We can't seed data to a specific partition via PutKvRequest yet.
        // Instead, verify that scanning each (partitionId, bucket) works and returns empty results without errors.
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucket);
            CLUSTER.waitUntilAllReplicaReady(tb);
            int leader = CLUSTER.waitAndGetLeader(tb);
            TabletServerGateway gw = CLUSTER.newTabletServerClientForNode(leader);

            ScanRequest openReq = newOpenScanReq(bucket, (int) partitionId, 8 * 1024);
            ScanResponse open = gw.scan(openReq).get();
            assertThat(open.hasErrorCode()).isFalse();
            long cnt = 0;
            if (open.hasRecords()) {
                cnt += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(open.getRecords()));
            }
            boolean more = open.hasHasMoreResults() && open.isHasMoreResults();
            byte[] scannerId = open.hasScannerId() ? open.getScannerId() : null;
            while (more) {
                if (scannerId != null) {
                    gw.scannerKeepAlive(new ScannerKeepAliveRequest().setScannerId(scannerId)).get();
                    ScanResponse page = gw.scan(nextPageReq(scannerId, 8 * 1024, false)).get();
                    if (page.hasRecords()) {
                        cnt += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(page.getRecords()));
                    }
                    more = page.hasHasMoreResults() && page.isHasMoreResults();
                } else {
                    break;
                }
            }
            assertThat(cnt).isEqualTo(0);
        }
    }

    @Test
    @Timeout(600)
    public void testKvScanMillionRecords() throws Exception {
        // Allow skipping in constrained environments
        boolean enabled = Boolean.parseBoolean(System.getProperty("fluss.runMillionScan", "true"));
        Assumptions.assumeTrue(enabled, "Skipping 1,000,000 records scan by system property");

        long tableId = createTable(CLUSTER, DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK);
        TableBucket tb = new TableBucket(tableId, 0);
        CLUSTER.waitUntilAllReplicaReady(tb);
        int leader = CLUSTER.waitAndGetLeader(tb);
        TabletServerGateway gw = CLUSTER.newTabletServerClientForNode(leader);

        // Seed 1,000,000 entries with very small values; batch inserts to keep memory/cpu reasonable
        seedKv(gw, tableId, 0, 1_000_000, 5_000);

        // Scan with moderate batch bytes and keep-alive
        ScanResponse open = gw.scan(newOpenScanReq(0, null, 64 * 1024)).get();
        assertThat(open.hasErrorCode()).isFalse();
        long total = 0;
        if (open.hasRecords()) {
            total += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(open.getRecords()));
        }
        byte[] scannerId = open.getScannerId();
        boolean more = open.isHasMoreResults();
        long lastKeepAlive = System.nanoTime();
        while (more) {
            long now = System.nanoTime();
            if (Duration.ofSeconds(10).toNanos() < (now - lastKeepAlive)) {
                gw.scannerKeepAlive(new ScannerKeepAliveRequest().setScannerId(scannerId)).get();
                lastKeepAlive = now;
            }
            ScanResponse page = gw.scan(nextPageReq(scannerId, 64 * 1024, false)).get();
            if (page.hasRecords()) {
                total += countRecordsInBatch(DefaultValueRecordBatch.pointToBytes(page.getRecords()));
            }
            more = page.isHasMoreResults();
        }
        assertThat(total).isEqualTo(1_000_000);
    }

    // ---------- helpers ----------

    private static void seedKv(TabletServerGateway gw, long tableId, int bucketId, int total, int batch) throws Exception {
        seedKv(gw, tableId, null, bucketId, total, batch);
    }

    private static void seedKv(TabletServerGateway gw, long tableId, Long partitionId, int bucketId, int total, int batch) throws Exception {
        RowType rowType = DATA1_ROW_TYPE;
        RowType pkType = DataTypes.ROW(new DataField("a", DataTypes.INT()));
        CompactedKeyEncoder keyEncoder = new CompactedKeyEncoder(rowType, new int[] {0});
        int inserted = 0;
        while (inserted < total) {
            int n = Math.min(batch, total - inserted);
            List<Tuple2<Object[], Object[]>> data = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                int id = inserted + i + 1;
                Object[] key = new Object[] {id};
                Object[] val = new Object[] {id, "v"};
                data.add(Tuple2.of(key, val));
            }
            DefaultKvRecordBatch kvBatch = (DefaultKvRecordBatch) DataTestUtils.genKvRecordBatch(pkType, rowType, data);
            gw.putKv(RpcMessageTestUtils.newPutKvRequest(tableId, bucketId, 1, kvBatch)).get();
            inserted += n;
        }
    }
}
