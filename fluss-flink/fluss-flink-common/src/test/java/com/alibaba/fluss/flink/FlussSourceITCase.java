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

package com.alibaba.fluss.flink;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.FlussSource;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FlussSourceITCase extends FlinkTestBase {

    private static final List<Order> collectedElements = new ArrayList<>();

    private static final List<Order> orders =
            Arrays.asList(
                    new Order(600, 20, 600, "addr1"),
                    new Order(700, 22, 601, "addr2"),
                    new Order(800, 23, 602, "addr3"),
                    new Order(900, 24, 603, "addr4"),
                    new Order(1000, 25, 604, "addr5"));

    private static final Schema schema = getOrdersSchema();

    private static Schema getOrdersSchema() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .primaryKey("orderId")
                .build();
    }

    @AfterAll
    protected static void afterEach() throws Exception {
        conn.close();
    }

    private void initOrdersTable() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "orderId") // Distribute by the id column with 1 buckets
                        .build();

        TablePath tablePath = TablePath.of("test-flink-db", "orders_test");
        try {
            admin.createTable(tablePath, tableDescriptor, false).get();
        } catch (Exception e) {
            System.out.println("Table creation failed, may already exist: " + e.getMessage());
        }

        TablePath ordersTablePath = new TablePath("test-flink-db", "orders_test");

        List<GenericRow> rows =
                orders.stream().map(RowConverters::pojoToGenericRow).collect(Collectors.toList());

        Table table = conn.getTable(ordersTablePath);

        for (GenericRow row : rows) {
            try {
                table.newUpsert().createWriter().upsert(row).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to insert test data", e);
            }
        }
    }

    @Test
    public void testSource() throws Exception {
        FlinkTestBase.beforeAll();

        initOrdersTable();
        String bootstrapServers =
                conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a DataStream from the FlussSource
        FlussSource<Order> flussSource =
                FlussSource.<Order>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase("test-flink-db")
                        .setTable("orders_test")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new OrderDeserializationSchema())
                        .build();

        DataStreamSource<Order> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        stream.addSink(
                new SinkFunction<Order>() {
                    @Override
                    public void invoke(Order value, Context context) {
                        collectedElements.add(value);
                    }
                });

        env.executeAsync("Test Fluss Source");
        // Wait a bit to collect results in the sink
        TimeUnit.SECONDS.sleep(1);

        // Assert result size and elements match
        Assertions.assertEquals(orders.size(), collectedElements.size());
        Assertions.assertEquals(orders, collectedElements);
    }

    public static class OrderDeserializationSchema implements FlussDeserializationSchema<Order> {
        @Override
        public Order deserialize(ScanRecord flussRecord) throws Exception {
            InternalRow row = flussRecord.getRow();
            long orderId = row.getLong(0);
            long itemId = row.getLong(1);
            int amount = row.getInt(2);
            String address = String.valueOf(row.getString(3));
            return new Order(orderId, itemId, amount, address);
        }

        @Override
        public TypeInformation<Order> getProducedType() {
            return TypeInformation.of(Order.class);
        }
    }
}
