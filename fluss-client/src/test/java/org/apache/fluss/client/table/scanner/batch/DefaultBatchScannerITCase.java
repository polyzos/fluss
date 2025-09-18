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

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.PartitionFilter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.client.table.scanner.batch.BatchScanUtils.collectRows;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for DefaultBatchScanner and Scan full-scan semantics. */
public class DefaultBatchScannerITCase extends ClientToServerITCaseBase {

    private static final Schema PK_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .primaryKey("id")
                    .build();

    private static final RowType PK_ROW_TYPE = PK_SCHEMA.getRowType();

    @Test
    void testFullScanPrimaryKeyTable_returnsAllRows() throws Exception {
        TablePath tablePath = TablePath.of("scan_db", "full_scan_pk");
        TableDescriptor desc =
                TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(3, "id").build();
        long tableId = createTable(tablePath, desc, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<Object[]> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                writer.upsert(row(i, "v" + i));
                expected.add(new Object[] {i, "v" + i});
            }
            writer.flush();

            // Full scan via Scan API (limit == null)
            BatchScanner scanner = table.newScan().createBatchScanner(new TableBucket(tableId, 0));
            List<InternalRow> actualRows = collectRows(scanner);

            // Assert count
            assertThat(actualRows).hasSize(10);

            // Assert content ignoring order
            List<Object[]> actual = toValues(actualRows, PK_ROW_TYPE);
            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testProjectionInFullScan() throws Exception {
        TablePath tablePath = TablePath.of("scan_db", "full_scan_pk_projection");
        TableDescriptor desc =
                TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(2, "id").build();
        long tableId = createTable(tablePath, desc, true);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            List<Object[]> expected = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                writer.upsert(row(i, "v" + i));
                expected.add(new Object[] {"v" + i});
            }
            writer.flush();

            int[] projected = new int[] {1};
            BatchScanner scanner =
                    table.newScan()
                            .project(projected)
                            .createBatchScanner(new TableBucket(tableId, 0));
            List<InternalRow> actualRows = collectRows(scanner);

            assertThat(actualRows).hasSize(5);
            List<Object[]> actual = toValues(actualRows, PK_ROW_TYPE.project(projected));
            assertThat(actual)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testFilterOnNonPartitionedTableThrows() throws Exception {
        TablePath tablePath = TablePath.of("scan_db", "full_scan_pk_filter_invalid");
        TableDescriptor desc =
                TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1, "id").build();
        long tableId = createTable(tablePath, desc, true);

        try (Table table = conn.getTable(tablePath)) {
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .filter(PartitionFilter.ofPartitionName("p=1"))
                                            .createBatchScanner(new TableBucket(tableId, 0)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "Partition filter is only supported for partitioned tables");
        }
    }

    @Test
    void testFullScanPartitionedTable_requiresPartitionFilter() throws Exception {
        TablePath tablePath = TablePath.of("scan_db", "full_scan_partitioned_pk");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .primaryKey("id", "pt")
                        .build();
        TableDescriptor desc =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("pt")
                        .distributedBy(3, "id")
                        .build();
        long tableId = createTable(tablePath, desc, true);

        // create partitions before writing
        admin.createPartition(tablePath, newPartitionSpec("pt", "a"), false).get();
        admin.createPartition(tablePath, newPartitionSpec("pt", "b"), false).get();

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            // write into two partitions pt=a and pt=b
            for (int i = 0; i < 4; i++) {
                writer.upsert(row(i, "va" + i, "a"));
                writer.upsert(row(i, "vb" + i, "b"));
            }
            writer.flush();

            // Full scan without filter should throw IllegalArgumentException from
            // DefaultBatchScanner
            assertThatThrownBy(
                            () ->
                                    table.newScan()
                                            .createBatchScanner(new TableBucket(tableId, 0))
                                            .pollBatch(java.time.Duration.ofMillis(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requires a PartitionFilter with a partition name");

            // With filter for partition a
            BatchScanner scannerA =
                    table.newScan()
                            .filter(PartitionFilter.ofPartitionName("a"))
                            .createBatchScanner(new TableBucket(tableId, 0));
            List<InternalRow> rowsA = collectRows(scannerA);
            assertThat(rowsA).hasSize(4);
            List<Object[]> valuesA = toValues(rowsA, schema.getRowType());
            assertThat(valuesA)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyInAnyOrder(
                            new Object[] {0, "va0", "a"},
                            new Object[] {1, "va1", "a"},
                            new Object[] {2, "va2", "a"},
                            new Object[] {3, "va3", "a"});

            // With filter for partition b
            BatchScanner scannerB =
                    table.newScan()
                            .filter(PartitionFilter.ofPartitionName("b"))
                            .createBatchScanner(new TableBucket(tableId, 0));
            List<InternalRow> rowsB = collectRows(scannerB);
            assertThat(rowsB).hasSize(4);
            List<Object[]> valuesB = toValues(rowsB, schema.getRowType());
            assertThat(valuesB)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyInAnyOrder(
                            new Object[] {0, "vb0", "b"},
                            new Object[] {1, "vb1", "b"},
                            new Object[] {2, "vb2", "b"},
                            new Object[] {3, "vb3", "b"});
        }
    }

    @Test
    void testFullScanOnLogTableThrows() throws Exception {
        // Non-PK table shouldn't support full scan BatchScanner
        TablePath tablePath = TablePath.of("scan_db", "full_scan_log_table_invalid");
        Schema logSchema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor desc =
                TableDescriptor.builder().schema(logSchema).distributedBy(1, "id").build();
        long tableId = createTable(tablePath, desc, true);

        try (Table table = conn.getTable(tablePath)) {
            assertThatThrownBy(
                            () -> table.newScan().createBatchScanner(new TableBucket(tableId, 0)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(
                            "Full scan BatchScanner is only supported for primary key tables.");
        }
    }

    private static List<Object[]> toValues(List<InternalRow> rows, RowType rowType) {
        List<Object[]> values = new ArrayList<>();
        Map<Integer, InternalRow.FieldGetter> getters = new HashMap<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            getters.put(i, InternalRow.createFieldGetter(rowType.getTypeAt(i), i));
        }
        for (InternalRow row : rows) {
            Object[] v = new Object[rowType.getFieldCount()];
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                Object field = getters.get(i).getFieldOrNull(row);
                // Normalize internal types for assertion comparisons
                if (field instanceof org.apache.fluss.row.BinaryString) {
                    field = field.toString();
                }
                v[i] = field;
            }
            values.add(v);
        }
        return values;
    }
}
