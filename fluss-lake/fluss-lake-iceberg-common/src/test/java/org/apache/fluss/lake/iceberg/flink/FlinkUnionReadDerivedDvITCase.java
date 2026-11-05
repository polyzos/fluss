/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Union read of derived deletion-vector tables ({@code table.deletion-vectors.mode = derived}) on
 * Iceberg v2 (file-scoped position deletes) and v3 (deletion vectors): rows updated and deleted
 * after they were tiered must not surface, through the union read or in the lake itself.
 */
class FlinkUnionReadDerivedDvITCase extends FlinkUnionReadTestBase {

    private static final int BUCKETS = 3;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
        // Derived commits write the position index under the remote data dir; the tiering job
        // takes its Fluss config from clientConf, which has no remote dir by default.
        clientConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());
    }

    @ParameterizedTest
    @CsvSource({"2, false", "2, true", "3, false", "3, true"})
    void testUpdatesAndDeletesOfTieredRows(int formatVersion, boolean partitioned)
            throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_v" + formatVersion + (partitioned ? "_part" : "");
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            createDerivedTable(tablePath, formatVersion, partitioned);
            List<String> parts =
                    partitioned ? Arrays.asList("p0", "p1") : Collections.singletonList(null);

            // round 1: keys 0..9 tiered to Iceberg
            List<InternalRow> rows = new ArrayList<>();
            for (String p : parts) {
                for (int i = 0; i < 10; i++) {
                    rows.add(rowOf(i, "v_" + i, p));
                }
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, parts.get(0));

            // round 2: update 0..2, delete 3..4, delete and re-insert 5
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (String p : parts) {
                    for (int i = 0; i < 3; i++) {
                        writer.upsert(rowOf(i, "updated_" + i, p));
                    }
                    writer.delete(rowOf(3, "v_3", p));
                    writer.delete(rowOf(4, "v_4", p));
                    writer.delete(rowOf(5, "v_5", p));
                    writer.upsert(rowOf(5, "reinserted_5", p));
                }
                writer.flush();
            }

            List<String> expected = new ArrayList<>();
            for (String p : parts) {
                for (int i : new int[] {0, 1, 2, 5, 6, 7, 8, 9}) {
                    String value = i < 3 ? "updated_" + i : (i == 5 ? "reinserted_5" : "v_" + i);
                    expected.add(rowString(i, value, p));
                }
            }
            expected.add(rowString(-1, "tick", parts.get(0)));

            // Before round 2 is tiered: the masks must hide the old versions.
            assertUnionRead(tableName, expected);

            // After round 2 is tiered: the deletes are in the same commit as the data, so the
            // union read and the lake on its own are both exact.
            tickUntilReadableAdvances(tablePath, readable, parts.get(0));
            assertUnionRead(tableName, expected);
            String columns = partitioned ? "c1, c2, c3" : "c1, c2";
            CloseableIterator<Row> lakeIter =
                    batchTEnv
                            .executeSql("select " + columns + " from " + tableName + "$lake")
                            .collect();
            assertResultsIgnoreOrder(lakeIter, expected, true);
        } finally {
            jobClient.cancel().get();
        }
    }

    /**
     * An idle table whose lake files another engine rewrote (e.g. Spark's rewrite_data_files) is
     * re-indexed without new writes, and its readable snapshot advances to the rewrite.
     */
    @ParameterizedTest
    @ValueSource(ints = {2, 3})
    void testIdleTableRewrittenByAnotherEngine(int formatVersion) throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            String tableName = "derived_idle_rewrite_v" + formatVersion;
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            long tableId = createDerivedTable(tablePath, formatVersion, false);

            List<InternalRow> rows = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                rows.add(rowOf(i, "v_" + i, null));
            }
            writeRows(tablePath, rows, false);
            long readable = tickUntilReadableAdvances(tablePath, Long.MIN_VALUE, null);
            // a second round leaves deletes in the lake, so the rewrite drops masked rows
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(0, "updated_0", null));
                writer.delete(rowOf(1, "v_1", null));
                writer.flush();
            }
            tickUntilReadableAdvances(tablePath, readable, null);
            long beforeRewrite = waitUntilAllTiered(tablePath, tableId);

            // the writers are idle from here on; another engine compacts the lake table
            long rewriteSnapshotId = rewriteAllDataFiles(tablePath);
            assertThat(rewriteSnapshotId).isNotEqualTo(beforeRewrite);
            waitUntil(
                    () ->
                            admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()
                                    == rewriteSnapshotId,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(200),
                    "readable snapshot of idle " + tablePath + " never moved to the rewrite");

            List<String> expected = new ArrayList<>();
            expected.add(rowString(-1, "tick", null));
            expected.add(rowString(0, "updated_0", null));
            for (int i = 2; i < 10; i++) {
                expected.add(rowString(i, "v_" + i, null));
            }
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, "c1, c2", expected);

            // The next round resolves superseded rows against the rewritten files' index.
            try (Table table = conn.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(rowOf(2, "updated_2", null));
                writer.delete(rowOf(3, "v_3", null));
                writer.flush();
            }
            expected.remove(rowString(2, "v_2", null));
            expected.remove(rowString(3, "v_3", null));
            expected.add(rowString(2, "updated_2", null));
            tickUntilReadableAdvances(tablePath, rewriteSnapshotId, null);
            assertUnionRead(tableName, expected);
            assertLakeRead(tableName, "c1, c2", expected);
        } finally {
            jobClient.cancel().get();
        }
    }

    /** Waits until every write is tiered and readable; returns the readable snapshot. */
    private long waitUntilAllTiered(TablePath tablePath, long tableId) {
        return waitValue(
                () -> {
                    LakeSnapshot snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    for (int b = 0; b < BUCKETS; b++) {
                        TableBucket tb = new TableBucket(tableId, b);
                        long highWatermark = getLeaderReplica(tb).getLogHighWatermark();
                        if (snapshot.getTableBucketsOffset().getOrDefault(tb, 0L) < highWatermark) {
                            Thread.sleep(200);
                            return Optional.empty();
                        }
                    }
                    return Optional.of(snapshot.getSnapshotId());
                },
                Duration.ofMinutes(2),
                "writes of " + tablePath + " not all tiered");
    }

    /**
     * Rewrites every live data file of the lake table into one file per bucket, dropping the delete
     * files, like a compaction by another engine; returns the rewrite's snapshot.
     */
    private long rewriteAllDataFiles(TablePath tablePath) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        long baseSnapshotId = table.currentSnapshot().snapshotId();
        Map<String, DataFile> oldDataFiles = new HashMap<>();
        Map<String, DeleteFile> oldDeleteFiles = new HashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                oldDataFiles.put(task.file().location(), task.file());
                for (DeleteFile delete : task.deletes()) {
                    oldDeleteFiles.put(delete.location(), delete);
                }
            }
        }
        Map<Integer, List<Record>> liveByBucket = new TreeMap<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                liveByBucket
                        .computeIfAbsent(
                                (Integer) record.getField(BUCKET_COLUMN_NAME),
                                k -> new ArrayList<>())
                        .add(record.copy());
            }
        }
        RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(baseSnapshotId);
        oldDataFiles.values().forEach(rewrite::deleteFile);
        oldDeleteFiles.values().forEach(rewrite::deleteFile);
        for (Map.Entry<Integer, List<Record>> e : liveByBucket.entrySet()) {
            TaskWriter<Record> writer =
                    TaskWriterFactory.createTaskWriter(table, null, e.getKey(), true);
            for (Record record : e.getValue()) {
                writer.write(record);
            }
            for (DataFile file : writer.complete().dataFiles()) {
                rewrite.addFile(file);
            }
        }
        rewrite.commit();
        table.refresh();
        return table.currentSnapshot().snapshotId();
    }

    private void assertLakeRead(String tableName, String columns, List<String> expected)
            throws Exception {
        CloseableIterator<Row> lakeIter =
                batchTEnv
                        .executeSql("select " + columns + " from " + tableName + "$lake")
                        .collect();
        assertResultsIgnoreOrder(lakeIter, expected, true);
    }

    private void assertUnionRead(String tableName, List<String> expected) throws Exception {
        CloseableIterator<Row> rowIter =
                batchTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(rowIter, expected, true);
    }

    private long createDerivedTable(TablePath tablePath, int formatVersion, boolean partitioned)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder().column("c1", DataTypes.INT()).column("c2", DataTypes.STRING());
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .distributedBy(BUCKETS)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_MODE.key(), "derived")
                        .customProperty("iceberg.format-version", String.valueOf(formatVersion));
        if (partitioned) {
            schema.column("c3", DataTypes.STRING()).primaryKey("c1", "c3");
            builder.partitionedBy("c3");
        } else {
            schema.primaryKey("c1");
        }
        return createTable(tablePath, builder.schema(schema.build()).build());
    }

    /** Upserts a tick row until the readable snapshot advances. */
    private long tickUntilReadableAdvances(
            TablePath tablePath, long previous, @Nullable String partition) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                long id = admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId();
                if (id != previous) {
                    return id;
                }
            } catch (Exception e) {
                // no readable snapshot yet
            }
            writeRows(tablePath, Collections.singletonList(rowOf(-1, "tick", partition)), false);
            Thread.sleep(1000);
        }
        throw new AssertionError("readable snapshot of " + tablePath + " stuck at " + previous);
    }

    private static InternalRow rowOf(int key, String value, @Nullable String partition) {
        return partition == null ? row(key, value) : row(key, value, partition);
    }

    private static String rowString(int key, String value, @Nullable String partition) {
        return partition == null
                ? String.format("+I[%d, %s]", key, value)
                : String.format("+I[%d, %s, %s]", key, value, partition);
    }
}
