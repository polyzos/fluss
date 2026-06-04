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

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.source.FlussSource;
import org.apache.fluss.flink.source.FlussSourceBuilder;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for union read through the DataStream {@link FlussSource}.
 *
 * <p>These tests mirror the Flink SQL union-read coverage ({@code FlinkUnionReadLogTableITCase} and
 * {@code FlinkUnionReadPrimaryKeyTableITCase}) but exercise the programmatic DataStream source.
 * Each test asserts the three properties that make a union read meaningful:
 *
 * <ol>
 *   <li>the first batch is physically tiered to Iceberg (verified by reading Iceberg directly);
 *   <li>a second batch written after the tiering job stops lives only in Fluss (verified by
 *       asserting Iceberg still holds exactly the first batch);
 *   <li>a {@code full()} DataStream source returns the union of the Iceberg historical data and the
 *       real-time Fluss data.
 * </ol>
 */
public class FlinkUnionReadDataStreamITCase extends FlinkUnionReadTestBase {

    private static final int[] FULL_COLS = new int[] {0, 1, 2};
    private static final int[] FULL_PARTITIONED_COLS = new int[] {0, 1, 2, 3};
    private static final int[] PROJECTED_ID_AMOUNT_COLS = new int[] {0, 2};

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTable(boolean isPartitioned) throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_log_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, isPartitioned);
        List<String> partitions = partitionsOf(tablePath, isPartitioned);

        // first batch: written to Fluss and tiered to Iceberg
        List<Row> firstBatch = new ArrayList<>();
        for (String partition : partitions) {
            firstBatch.addAll(appendLogRows(tablePath, 0, 5, partition));
        }
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // (1) verify the first batch is actually in Iceberg
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        // second batch: written to Fluss only, after tiering has stopped
        tieringJob.cancel().get();
        List<Row> secondBatch = new ArrayList<>();
        for (String partition : partitions) {
            secondBatch.addAll(appendLogRows(tablePath, 100, 3, partition));
        }

        // (2) verify the second batch is NOT in Iceberg (still only the first batch is tiered)
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        // (3) union read must return the Iceberg historical data + the Fluss-only data
        List<Row> expected = new ArrayList<>(firstBatch);
        expected.addAll(secondBatch);
        FlussSource<RowData> source = buildSource(tableName);
        List<Row> actual =
                collect(source, expected.size(), isPartitioned ? FULL_PARTITIONED_COLS : FULL_COLS);

        assertRowsIgnoreOrder(actual, expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadPrimaryKeyTable(boolean isPartitioned) throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName = "ds_union_pk_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createPkTable(tablePath, isPartitioned);
        List<String> partitions = partitionsOf(tablePath, isPartitioned);

        // first batch: three distinct keys per partition, tiered to Iceberg as the snapshot
        List<Row> snapshotRows = new ArrayList<>();
        for (String partition : partitions) {
            snapshotRows.addAll(upsertRows(tablePath, 1, 3, partition));
        }
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // (1) verify the snapshot (latest value per key) is actually in Iceberg
        assertIcebergContainsExactly(tablePath, isPartitioned, snapshotRows);

        // after tiering stops, update one existing key per partition. The update lives only in the
        // Fluss changelog and is read as -U/+U on top of the Iceberg snapshot read as +I.
        tieringJob.cancel().get();
        List<Row> changelog = new ArrayList<>();
        for (String partition : partitions) {
            changelog.addAll(updateRow(tablePath, 1, partition));
        }

        // (2) verify the update is NOT in Iceberg: it must still hold the original snapshot values
        assertIcebergContainsExactly(tablePath, isPartitioned, snapshotRows);

        // (3) union read must return the Iceberg snapshot (+I) and the Fluss-only changelog (-U/+U)
        List<Row> expected = new ArrayList<>(snapshotRows);
        expected.addAll(changelog);
        FlussSource<RowData> source = buildSource(tableName);
        List<Row> actual =
                collect(source, expected.size(), isPartitioned ? FULL_PARTITIONED_COLS : FULL_COLS);

        assertRowsIgnoreOrder(actual, expected);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadWithProjectionPushdown(boolean isPartitioned) throws Exception {
        JobClient tieringJob = buildTieringJob(execEnv);

        String tableName =
                "ds_union_projection_" + (isPartitioned ? "partitioned" : "non_partitioned");
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId = createLogTable(tablePath, isPartitioned);
        List<String> partitions = partitionsOf(tablePath, isPartitioned);

        List<Row> firstBatch = new ArrayList<>();
        for (String partition : partitions) {
            firstBatch.addAll(appendLogRows(tablePath, 0, 5, partition));
        }
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // (1) verify the first batch is actually in Iceberg
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        tieringJob.cancel().get();
        List<Row> secondBatch = new ArrayList<>();
        for (String partition : partitions) {
            secondBatch.addAll(appendLogRows(tablePath, 100, 3, partition));
        }

        // (2) verify the second batch is NOT in Iceberg (Fluss-only)
        assertIcebergContainsExactly(tablePath, isPartitioned, firstBatch);

        // (3) union read with projection must return only (id, amount) for both sources
        List<Row> written = new ArrayList<>(firstBatch);
        written.addAll(secondBatch);
        FlussSource<RowData> source = buildSource(tableName, "id", "amount");
        List<Row> actual = collect(source, written.size(), PROJECTED_ID_AMOUNT_COLS);

        List<Row> expected =
                written.stream()
                        .map(r -> Row.ofKind(RowKind.INSERT, r.getField(0), r.getField(2)))
                        .collect(Collectors.toList());

        assertRowsIgnoreOrder(actual, expected);
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private FlussSource<RowData> buildSource(String tableName, String... projectedFields) {
        FlussSourceBuilder<RowData> builder =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.full())
                        .setScanPartitionDiscoveryIntervalMs(100L)
                        .setDeserializationSchema(new RowDataDeserializationSchema());
        if (projectedFields.length > 0) {
            builder.setProjectedFields(projectedFields);
        }
        return builder.build();
    }

    private List<Row> collect(FlussSource<RowData> source, int expectedCount, int[] cols)
            throws Exception {
        DataStreamSource<RowData> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Fluss Union Source");
        return stream.executeAndCollect(expectedCount).stream()
                .map(rowData -> toRow(rowData, cols))
                .collect(Collectors.toList());
    }

    private List<String> partitionsOf(TablePath tablePath, boolean isPartitioned) {
        if (!isPartitioned) {
            return Collections.singletonList(null);
        }
        return new ArrayList<>(waitUntilPartitions(tablePath).values());
    }

    private List<Row> appendLogRows(
            TablePath tablePath, int startId, int count, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> expected = new ArrayList<>();
        for (int id = startId; id < startId + count; id++) {
            String name = "name_" + id;
            long amount = id * 10L;
            rows.add(internalRow(id, name, amount, partition));
            expected.add(expectedRow(RowKind.INSERT, id, name, amount, partition));
        }
        writeRows(tablePath, rows, true);
        return expected;
    }

    private List<Row> upsertRows(
            TablePath tablePath, int startId, int count, @Nullable String partition)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> expected = new ArrayList<>();
        for (int id = startId; id < startId + count; id++) {
            String name = "name_" + id;
            long amount = id * 10L;
            rows.add(internalRow(id, name, amount, partition));
            expected.add(expectedRow(RowKind.INSERT, id, name, amount, partition));
        }
        writeRows(tablePath, rows, false);
        return expected;
    }

    private List<Row> updateRow(TablePath tablePath, int id, @Nullable String partition)
            throws Exception {
        String newName = "updated_" + id;
        long newAmount = id * 100L;
        writeRows(
                tablePath,
                Collections.singletonList(internalRow(id, newName, newAmount, partition)),
                false);

        List<Row> expected = new ArrayList<>();
        expected.add(expectedRow(RowKind.UPDATE_BEFORE, id, "name_" + id, id * 10L, partition));
        expected.add(expectedRow(RowKind.UPDATE_AFTER, id, newName, newAmount, partition));
        return expected;
    }

    private static InternalRow internalRow(
            int id, String name, long amount, @Nullable String partition) {
        return partition == null ? row(id, name, amount) : row(id, name, amount, partition);
    }

    private static Row expectedRow(
            RowKind kind, int id, String name, long amount, @Nullable String partition) {
        return partition == null
                ? Row.ofKind(kind, id, name, amount)
                : Row.ofKind(kind, id, name, amount, partition);
    }

    private static Row toRow(RowData rowData, int[] cols) {
        Object[] fields = new Object[cols.length];
        for (int i = 0; i < cols.length; i++) {
            if (rowData.isNullAt(i)) {
                fields[i] = null;
                continue;
            }
            switch (cols[i]) {
                case 0:
                    fields[i] = rowData.getInt(i);
                    break;
                case 1:
                    fields[i] = rowData.getString(i).toString();
                    break;
                case 2:
                    fields[i] = rowData.getLong(i);
                    break;
                case 3:
                    fields[i] = rowData.getString(i).toString();
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected column index: " + cols[i]);
            }
        }
        return Row.ofKind(rowData.getRowKind(), fields);
    }

    private static void assertRowsIgnoreOrder(List<Row> actual, List<Row> expected) {
        assertThat(actual.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        expected.stream().map(Row::toString).collect(Collectors.toList()));
    }

    /**
     * Asserts that the data physically present in Iceberg is exactly {@code expectedValues}
     * (ignoring order). Because tiering is asynchronous, it first waits until the expected number
     * of rows is visible. Comparing for exact equality both proves that the tiered batch is in
     * Iceberg and that no later (Fluss-only) data has leaked into Iceberg.
     */
    private void assertIcebergContainsExactly(
            TablePath tablePath, boolean isPartitioned, List<Row> expectedValues) throws Exception {
        waitUntil(
                () -> readIcebergUserRows(tablePath, isPartitioned).size() == expectedValues.size(),
                Duration.ofMinutes(2),
                // poll once per second: each probe opens Iceberg/Parquet readers, so a tight loop
                // would exhaust file descriptors
                Duration.ofSeconds(1),
                "Iceberg did not contain the expected number of rows for " + tablePath);
        List<Row> icebergValues = readIcebergUserRows(tablePath, isPartitioned);
        // expected rows carry an INSERT kind; compare on values only by normalizing both sides
        assertThat(icebergValues.stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrderElementsOf(
                        expectedValues.stream()
                                .map(r -> Row.of(toArray(r)).toString())
                                .collect(Collectors.toList()));
    }

    private List<Row> readIcebergUserRows(TablePath tablePath, boolean isPartitioned)
            throws Exception {
        // Read Iceberg directly via a full generic scan. This reads all partitions and applies
        // primary-key deletes (so a PK table yields the latest value per key), and avoids the
        // shared test util's log-table reader which de-duplicates data files by __offset and would
        // drop same-offset files from different partitions.
        Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        List<Row> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                int id = ((Number) record.getField("id")).intValue();
                Object nameField = record.getField("name");
                String name = nameField == null ? null : nameField.toString();
                long amount = ((Number) record.getField("amount")).longValue();
                if (isPartitioned) {
                    Object partitionField = record.getField("p");
                    String partition = partitionField == null ? null : partitionField.toString();
                    rows.add(Row.of(id, name, amount, partition));
                } else {
                    rows.add(Row.of(id, name, amount));
                }
            }
        }
        return rows;
    }

    private static Object[] toArray(Row row) {
        Object[] fields = new Object[row.getArity()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = row.getField(i);
        }
        return fields;
    }

    private long createLogTable(TablePath tablePath, boolean isPartitioned) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            tableBuilder
                    .partitionedBy("p")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.distributedBy(DEFAULT_BUCKET_NUM, "id").schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private long createPkTable(TablePath tablePath, boolean isPartitioned) throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("amount", DataTypes.BIGINT());
        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(DEFAULT_BUCKET_NUM)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            schemaBuilder.primaryKey("id", "p");
            tableBuilder
                    .partitionedBy("p")
                    .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                    .property(
                            ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                            AutoPartitionTimeUnit.YEAR);
        } else {
            schemaBuilder.primaryKey("id");
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private static String bootstrapServers() {
        return String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
    }
}
