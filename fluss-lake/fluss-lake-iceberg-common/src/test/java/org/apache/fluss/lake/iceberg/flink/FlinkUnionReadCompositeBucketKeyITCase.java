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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;

/**
 * Flink test for an Iceberg-tiered primary key table whose bucket key has multiple columns: writes
 * through the Flink sink, union reads over the lake snapshot and the log tail, and lookup joins on
 * the composite bucket key.
 */
class FlinkUnionReadCompositeBucketKeyITCase extends FlinkUnionReadTestBase {

    private static final int BUCKET_NUM = 3;

    @Test
    void testUnionReadAndLookupJoin() throws Exception {
        String tableName = "order_items_composite";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        streamTEnv.executeSql(
                String.format(
                        "CREATE TABLE %s ("
                                + " order_id BIGINT,"
                                + " tenant_id BIGINT,"
                                + " item_id BIGINT,"
                                + " amount STRING,"
                                + " PRIMARY KEY (order_id, tenant_id, item_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'bucket.num' = '%s',"
                                + " 'bucket.key' = 'order_id,tenant_id',"
                                + " 'table.datalake.enabled' = 'true',"
                                + " 'table.datalake.freshness' = '500ms')",
                        tableName, BUCKET_NUM));
        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // write through the Flink sink, which shuffles rows by the bucket key; enough orders so
        // that every bucket has data and gets a lake snapshot
        List<String> expected = new ArrayList<>();
        insertRows(tableName, 1, 20, expected);

        JobClient jobClient = buildTieringJob(execEnv);
        try {
            waitUntilBucketSynced(tablePath, tableId, BUCKET_NUM, false);
            // rows written after the first lake snapshot are read from the log tail
            insertRows(tableName, 21, 25, expected);

            assertResultsIgnoreOrder(
                    streamTEnv.executeSql("SELECT * FROM " + tableName).collect(), expected, true);
            assertResultsIgnoreOrder(
                    streamTEnv
                            .executeSql("SELECT * FROM " + tableName + " WHERE order_id = 3")
                            .collect(),
                    Arrays.asList(
                            "+I[3, 1, 1, o3-t1-i1]",
                            "+I[3, 1, 2, o3-t1-i2]",
                            "+I[3, 2, 1, o3-t2-i1]",
                            "+I[3, 2, 2, o3-t2-i2]"),
                    true);
        } finally {
            jobClient.cancel().get();
        }

        // a lookup join on the composite bucket key is a prefix lookup that returns only the items
        // of the given (order_id, tenant_id); the join condition order doesn't matter
        streamTEnv.executeSql(
                "CREATE TEMPORARY VIEW probes AS SELECT order_id, tenant_id, proctime() AS proc "
                        + "FROM (VALUES (CAST(4 AS BIGINT), CAST(2 AS BIGINT)), "
                        + "(CAST(22 AS BIGINT), CAST(1 AS BIGINT))) AS T(order_id, tenant_id)");
        assertResultsIgnoreOrder(
                streamTEnv
                        .executeSql(
                                String.format(
                                        "SELECT p.order_id, p.tenant_id, i.item_id, i.amount "
                                                + "FROM probes AS p JOIN %s "
                                                + "FOR SYSTEM_TIME AS OF p.proc AS i "
                                                + "ON p.tenant_id = i.tenant_id "
                                                + "AND p.order_id = i.order_id",
                                        tableName))
                        .collect(),
                Arrays.asList(
                        "+I[4, 2, 1, o4-t2-i1]",
                        "+I[4, 2, 2, o4-t2-i2]",
                        "+I[22, 1, 1, o22-t1-i1]",
                        "+I[22, 1, 2, o22-t1-i2]"),
                true);
    }

    private void insertRows(String tableName, int fromOrder, int toOrder, List<String> expected)
            throws Exception {
        List<String> values = new ArrayList<>();
        for (int order = fromOrder; order <= toOrder; order++) {
            for (int tenant = 1; tenant <= 2; tenant++) {
                for (int item = 1; item <= 2; item++) {
                    String amount = String.format("o%s-t%s-i%s", order, tenant, item);
                    values.add(
                            String.format(
                                    "(CAST(%s AS BIGINT), CAST(%s AS BIGINT), CAST(%s AS BIGINT), '%s')",
                                    order, tenant, item, amount));
                    expected.add(String.format("+I[%s, %s, %s, %s]", order, tenant, item, amount));
                }
            }
        }
        streamTEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s VALUES %s", tableName, String.join(", ", values)))
                .await();
    }
}
