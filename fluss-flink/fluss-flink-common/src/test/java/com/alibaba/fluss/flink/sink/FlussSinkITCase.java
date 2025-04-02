/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.source.deserializer.Order;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FlussSinkITCase extends FlinkTestBase {
    private static final Schema pkSchema =
            Schema.newBuilder()
                    .column("orderId", DataTypes.BIGINT())
                    .column("itemId", DataTypes.BIGINT())
                    .column("amount", DataTypes.INT())
                    .column("address", DataTypes.STRING())
                    .primaryKey("orderId")
                    .build();

    private static final List<Order> orders =
            Arrays.asList(
                    new Order(600, 20, 600, "addr1"),
                    new Order(700, 22, 601, "addr2"),
                    new Order(800, 23, 602, "addr3"),
                    new Order(900, 24, 603, "addr4"),
                    new Order(1000, 25, 604, "addr5"));

    private static TableDescriptor pkTableDescriptor;

    private static String bootstrapServers;

    private static String pkTableName = "orders_test_pk";

    private static TablePath ordersPKTablePath;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        pkTableDescriptor =
                TableDescriptor.builder().schema(pkSchema).distributedBy(1, "orderId").build();

        TablePath pkTablePath = TablePath.of(DEFAULT_DB, pkTableName);

        createTable(pkTablePath, pkTableDescriptor);

        this.ordersPKTablePath = new TablePath(DEFAULT_DB, pkTableName);

        this.bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        conn.close();
    }

    @Test
    public void testOrdersTablePKSink() throws Exception {
        FlinkTestBase.beforeAll();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a DataStream from the FlussSource
        DataStream<Order> stream = env.fromData(orders);

        //        TableInfo tableInfo = admin.getTableInfo(new TablePath(DEFAULT_DB,
        // pkTableName)).get();

        // Define the RowType that exactly matches your Fluss table schema
        RowType rowType =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), // id
                            new BigIntType(), // age
                            new IntType(),
                            new VarCharType(true, VarCharType.MAX_LENGTH)
                        },
                        new String[] {"orderId", "itemId", "amount", "address"});

        FlinkSink<Order> flussSink =
                FlussSink.<Order>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .useUpsert()
                        .setRowType(rowType)
                        .setInputType(Order.class) // Signals that no conversion is needed
                        .build();

        stream.sinkTo(flussSink).name("Fluss Sink");

        env.executeAsync("Test Orders Fluss Sink");

        Table table = conn.getTable(new TablePath(DEFAULT_DB, pkTableName));
        LogScanner logScanner = table.newScan().createLogScanner();

        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }

        List<GenericRow> rows = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    GenericRow row = (GenericRow) record.getRow();
                    rows.add(row);
                }
            }
        }

        List<Order> expectedResult =
                rows.stream()
                        .map(
                                row -> {
                                    long orderId = row.getLong(0);
                                    long itemId = row.getLong(1);
                                    int amount = row.getInt(2);

                                    String address = String.valueOf(row.getString(3));

                                    return new Order(orderId, itemId, amount, address);
                                })
                        .collect(Collectors.toList());

        // Assert result size and elements match
        Assertions.assertEquals(expectedResult.size(), orders.size());
        Assertions.assertEquals(expectedResult, orders);
    }
}
