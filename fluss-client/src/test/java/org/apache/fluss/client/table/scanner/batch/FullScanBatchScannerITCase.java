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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.TableScan;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.TableNotPartitionedException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end IT cases for FullScanBatchScanner functionality. */
class FullScanBatchScannerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initClusterConfig())
                    .build();

    private Connection conn;
    private Admin admin;
    private Configuration clientConf;

    private static Configuration initClusterConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // faster snapshotting in tests
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // writer settings to keep buffers small
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.MAX_PARTITION_NUM, 10);
        conf.set(ConfigOptions.MAX_BUCKET_NUM, 30);
        return conf;
    }

    @BeforeEach
    void setUp() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void testSnapshotAllNotPartitioned() throws Exception {
        TablePath tablePath = TablePath.of("batch_full_scan", "non_partitioned_pk");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        TableDescriptor desc =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        // create db/table
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, desc, false).get();

        try (Table table = conn.getTable(tablePath)) {
            // write 10 rows across 3 buckets
            UpsertWriter upsert = table.newUpsert().createWriter();
            List<InternalRow> expected = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                InternalRow r = row(i, "v" + i);
                upsert.upsert(r);
                expected.add(r);
            }
            upsert.flush();

            // run snapshotAll() via BatchScanner
            TableScan scan = (TableScan) table.newScan();
            BatchScanner scanner = scan.createBatchScanner();
            CompletableFuture<List<InternalRow>> fut = scanner.snapshotAll();
            List<InternalRow> actual = fut.get();

            // verify count and contents (order-agnostic by sorting on primary key 'a')
            assertThat(actual).hasSize(10);
            RowType rowType = schema.getRowType();
            Comparator<InternalRow> byKey = Comparator.comparingInt(r -> r.getInt(0));
            List<InternalRow> sortedActual =
                    actual.stream().sorted(byKey).collect(Collectors.toList());
            List<InternalRow> sortedExpected =
                    expected.stream().sorted(byKey).collect(Collectors.toList());
            for (int i = 0; i < sortedActual.size(); i++) {
                assertThatRow(sortedActual.get(i))
                        .withSchema(rowType)
                        .isEqualTo(sortedExpected.get(i));
            }

            // unhappy path: createBatchScanner with partition on non-partitioned table
            assertThatThrownBy(
                            () ->
                                    ((org.apache.fluss.client.table.scanner.TableScan)
                                                    table.newScan())
                                            .createBatchScanner("p1"))
                    .isInstanceOf(TableNotPartitionedException.class)
                    .hasMessageContaining("Table is not partitioned");
        }
    }

    @Test
    void testSnapshotAllPartitioned() throws Exception {
        TablePath tablePath = TablePath.of("batch_full_scan", "partitioned_pk");
        // Partition by column 'p', and include 'p' in PK so physical PK = [a, p]
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("p", DataTypes.STRING())
                        .primaryKey("a", "p")
                        .build();
        TableDescriptor desc =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(3, "a")
                        .partitionedBy("p")
                        .build();

        // create db/table
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, desc, false).get();

        // explicitly create two partitions: p=20240101 and p=20240102
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("p", "20240101")),
                        true)
                .get();
        admin.createPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("p", "20240102")),
                        true)
                .get();

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsert = table.newUpsert().createWriter();
            List<InternalRow> expectedP1 = new ArrayList<>();
            List<InternalRow> expectedP2 = new ArrayList<>();
            for (int i = 1; i <= 10; i++) {
                InternalRow r1 = row(i, "v" + i, "20240101");
                upsert.upsert(r1);
                expectedP1.add(r1);
                InternalRow r2 = row(i, "w" + i, "20240102");
                upsert.upsert(r2);
                expectedP2.add(r2);
            }
            upsert.flush();

            // valid partition snapshots via BatchScanner
            List<InternalRow> p1Rows =
                    ((org.apache.fluss.client.table.scanner.TableScan) table.newScan())
                            .createBatchScanner("20240101")
                            .snapshotAll()
                            .get();
            List<InternalRow> p2Rows =
                    ((org.apache.fluss.client.table.scanner.TableScan) table.newScan())
                            .createBatchScanner("20240102")
                            .snapshotAll()
                            .get();

            RowType rowType = schema.getRowType();
            Comparator<InternalRow> byKey = Comparator.comparingInt(r -> r.getInt(0));

            // verify P1
            assertThat(p1Rows).hasSize(10);
            List<InternalRow> sortedP1 = p1Rows.stream().sorted(byKey).collect(Collectors.toList());
            List<InternalRow> sortedExpectedP1 =
                    expectedP1.stream().sorted(byKey).collect(Collectors.toList());
            for (int i = 0; i < sortedP1.size(); i++) {
                assertThatRow(sortedP1.get(i))
                        .withSchema(rowType)
                        .isEqualTo(sortedExpectedP1.get(i));
            }

            // verify P2
            assertThat(p2Rows).hasSize(10);
            List<InternalRow> sortedP2 = p2Rows.stream().sorted(byKey).collect(Collectors.toList());
            List<InternalRow> sortedExpectedP2 =
                    expectedP2.stream().sorted(byKey).collect(Collectors.toList());
            for (int i = 0; i < sortedP2.size(); i++) {
                assertThatRow(sortedP2.get(i))
                        .withSchema(rowType)
                        .isEqualTo(sortedExpectedP2.get(i));
            }

            // unhappy path: invalid partition name
            assertThatThrownBy(
                            () ->
                                    ((org.apache.fluss.client.table.scanner.TableScan)
                                                    table.newScan())
                                            .createBatchScanner("p=does_not_exist"))
                    .isInstanceOf(PartitionNotExistException.class)
                    .hasMessageContaining("does not exist");

            // unhappy path: snapshotAll on partitioned table without specifying partition
            assertThatThrownBy(
                            () ->
                                    ((org.apache.fluss.client.table.scanner.TableScan)
                                                    table.newScan())
                                            .createBatchScanner())
                    .isInstanceOf(TableNotPartitionedException.class)
                    .hasMessageContaining("Table is partitioned");
        }
    }

    // This test is mainly used for checking the latency when the table is around 10k values
    // which is fairly too large for the particular use case of snapshotAll().
    @Test
    void testSnapshotAllFor10KTable() throws Exception {
        TablePath tablePath = TablePath.of("batch_full_scan", "non_partitioned_pk_10k");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();
        TableDescriptor desc =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        // create db/table
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, desc, false).get();

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsert = table.newUpsert().createWriter();
            int total = 10_000;
            for (int i = 1; i <= total; i++) {
                upsert.upsert(row(i, "v" + i));
                if (i % 1000 == 0) {
                    // periodic flush to keep memory stable in CI
                    upsert.flush();
                }
            }
            upsert.flush();

            // run snapshotAll via BatchScanner
            List<InternalRow> rows =
                    ((org.apache.fluss.client.table.scanner.TableScan) table.newScan())
                            .createBatchScanner()
                            .snapshotAll()
                            .get();

            // verify size and key coverage
            assertThat(rows).hasSize(total);
            HashSet<Integer> keys = new HashSet<>(rows.size());
            for (InternalRow r : rows) {
                keys.add(r.getInt(0));
            }
            assertThat(keys).hasSize(total);
            // spot-check a few
            assertThat(keys.contains(1)).isTrue();
            assertThat(keys.contains(5000)).isTrue();
            assertThat(keys.contains(10000)).isTrue();
        }
    }
}
