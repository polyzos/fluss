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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for an Iceberg-tiered primary key table whose bucket key has multiple columns:
 * writes, prefix lookups on the composite bucket key, tiering, and Iceberg partition pruning on the
 * first bucket key column.
 */
class IcebergCompositeBucketKeyITCase extends FlinkIcebergTieringTestBase {

    private static final String DB = "fluss";
    private static final int BUCKET_NUM = 3;
    private static final int ORDERS = 20;
    private static final int TENANTS = 2;
    private static final int ITEMS = 3;

    private static final Schema ORDER_ITEMS_SCHEMA =
            Schema.newBuilder()
                    .column("order_id", DataTypes.BIGINT())
                    .column("tenant_id", DataTypes.BIGINT())
                    .column("item_id", DataTypes.BIGINT())
                    .column("amount", DataTypes.STRING())
                    .primaryKey("order_id", "tenant_id", "item_id")
                    .build();

    private static StreamExecutionEnvironment tieringEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkIcebergTieringTestBase.beforeAll();
        tieringEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        tieringEnv.setParallelism(2);
        tieringEnv.enableCheckpointing(1000);
    }

    @Test
    void testRejectUnsupportedBucketKeys() {
        Schema logSchema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("b", DataTypes.BIGINT())
                        .build();
        // a log table, a composite default bucket key, and a bucket key that is not a prefix
        TableDescriptor[] unsupported = {
            TableDescriptor.builder().schema(logSchema).distributedBy(3, "a", "b").build(),
            TableDescriptor.builder().schema(ORDER_ITEMS_SCHEMA).distributedBy(3).build(),
            TableDescriptor.builder()
                    .schema(ORDER_ITEMS_SCHEMA)
                    .distributedBy(3, "tenant_id", "order_id")
                    .build()
        };
        for (int i = 0; i < unsupported.length; i++) {
            TablePath tablePath = TablePath.of(DB, "unsupported_" + i);
            TableDescriptor descriptor = unsupported[i];
            // rejected even with the datalake disabled, since the Iceberg format is still set
            assertThatThrownBy(() -> admin.createTable(tablePath, descriptor, false).get())
                    .rootCause()
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("Iceberg tables only support a bucket key with multiple");
        }
    }

    @Test
    void testTieringAndLookupsWithCompositeBucketKey() throws Exception {
        TablePath tablePath = TablePath.of(DB, "order_items");
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(ORDER_ITEMS_SCHEMA)
                                .distributedBy(BUCKET_NUM, "order_id", "tenant_id")
                                .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                                .property(
                                        ConfigOptions.TABLE_DATALAKE_FRESHNESS,
                                        Duration.ofMillis(500))
                                .build());

        List<InternalRow> rows = new ArrayList<>();
        for (long order = 1; order <= ORDERS; order++) {
            for (long tenant = 1; tenant <= TENANTS; tenant++) {
                for (long item = 1; item <= ITEMS; item++) {
                    rows.add(row(order, tenant, item, amount(order, tenant, item)));
                }
            }
        }
        writeRows(tablePath, rows, false);

        // a prefix lookup on the full composite bucket key returns exactly the matching rows
        try (Table table = conn.getTable(tablePath)) {
            Lookuper prefixLookuper =
                    table.newLookup().lookupBy("order_id", "tenant_id").createLookuper();
            for (long order = 1; order <= ORDERS; order++) {
                for (long tenant = 1; tenant <= TENANTS; tenant++) {
                    LookupResult result = prefixLookuper.lookup(row(order, tenant)).get();
                    Set<String> amounts = new HashSet<>();
                    for (InternalRow resultRow : result.getRowList()) {
                        assertThat(resultRow.getLong(0)).isEqualTo(order);
                        assertThat(resultRow.getLong(1)).isEqualTo(tenant);
                        amounts.add(resultRow.getString(3).toString());
                    }
                    assertThat(amounts).hasSize(ITEMS);
                }
            }
        }

        JobClient jobClient = buildTieringJob(tieringEnv);
        try {
            waitUntilBucketSynced(tablePath, tableId, BUCKET_NUM, false);
        } finally {
            jobClient.cancel().get();
        }

        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        List<PartitionField> fields = icebergTable.spec().fields();
        assertThat(fields).hasSize(1);
        assertThat(fields.get(0).transform().toString()).isEqualTo("bucket[" + BUCKET_NUM + "]");
        assertThat(icebergTable.schema().findColumnName(fields.get(0).sourceId()))
                .isEqualTo("order_id");

        assertThat(readIcebergAmounts(icebergTable, null)).hasSize(rows.size());

        Transform<Object, Integer> bucketTransform = Transforms.bucket(BUCKET_NUM);
        Function<Object, Integer> icebergBucket = bucketTransform.bind(Types.LongType.get());
        for (long order = 1; order <= ORDERS; order++) {
            // Iceberg prunes an equality filter on the first bucket key column to the files of a
            // single bucket, which is the Fluss bucket the rows were written to
            int expectedBucket = icebergBucket.apply(order);
            try (CloseableIterable<FileScanTask> tasks =
                    icebergTable
                            .newScan()
                            .filter(Expressions.equal("order_id", order))
                            .planFiles()) {
                for (FileScanTask task : tasks) {
                    assertThat(task.file().partition().get(0, Integer.class))
                            .isEqualTo(expectedBucket);
                }
            }
            // and no row is lost by the pruning
            Set<String> expected = new HashSet<>();
            for (long tenant = 1; tenant <= TENANTS; tenant++) {
                for (long item = 1; item <= ITEMS; item++) {
                    expected.add(amount(order, tenant, item));
                }
            }
            assertThat(readIcebergAmounts(icebergTable, order))
                    .containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    private static List<String> readIcebergAmounts(
            org.apache.iceberg.Table icebergTable, Long orderId) throws Exception {
        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(icebergTable);
        if (orderId != null) {
            scan = scan.where(Expressions.equal("order_id", orderId));
        }
        List<String> amounts = new ArrayList<>();
        try (CloseableIterable<Record> records = scan.build()) {
            for (Record record : records) {
                if (orderId == null || orderId.equals(record.getField("order_id"))) {
                    amounts.add((String) record.getField("amount"));
                }
            }
        }
        return amounts;
    }

    private static String amount(long order, long tenant, long item) {
        return "o" + order + "-t" + tenant + "-i" + item;
    }
}
