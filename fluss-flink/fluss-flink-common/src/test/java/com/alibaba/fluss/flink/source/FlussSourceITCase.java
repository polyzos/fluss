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

package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.serdes.FlussDeserializationSchema;
import com.alibaba.fluss.flink.serdes.FlussRowDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.fluss.testutils.DataTestUtils.row;

public class FlussSourceITCase extends FlinkTestBase {
    private static final List<Order> orders =
            Arrays.asList(
                    new Order(600, 20, 600, "addr1"),
                    new Order(700, 22, 601, "addr2"),
                    new Order(800, 23, 602, "addr3"),
                    new Order(900, 24, 603, "addr4"),
                    new Order(1000, 25, 604, "addr5"));

    private static final Schema pkSchema = getOrdersSchemaPK();
    private static final Schema logSchema = getOrdersSchemaLog();

    private static TableDescriptor pkTableDescriptor;
    private static TableDescriptor logTableDescriptor;

    private static String bootstrapServers;

    private static final List<Order> collectedElements = new ArrayList<>();
    private static final List<RowData> collectedRows = new ArrayList<>();

    private static String pkTableName = "orders_test_pk";
    private static String logTableName = "orders_test_log";

    private static Schema getOrdersSchemaPK() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .primaryKey("orderId")
                .build();
    }

    private static Schema getOrdersSchemaLog() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .build();
    }

    private static TablePath ordersPKTablePath;
    private static TablePath ordersLogTablePath;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        pkTableDescriptor =
                TableDescriptor.builder().schema(pkSchema).distributedBy(1, "orderId").build();

        logTableDescriptor =
                TableDescriptor.builder().schema(logSchema).distributedBy(1, "orderId").build();

        TablePath pkTablePath = TablePath.of(DEFAULT_DB, pkTableName);
        TablePath logTablePath = TablePath.of(DEFAULT_DB, logTableName);

        createTable(pkTablePath, pkTableDescriptor);
        createTable(logTablePath, logTableDescriptor);

        ordersPKTablePath = new TablePath(DEFAULT_DB, pkTableName);
        ordersLogTablePath = new TablePath(DEFAULT_DB, logTableName);

        initTables();
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);
    }

    @AfterAll
    protected static void afterAll() throws Exception {
        conn.close();
    }

    @Test
    public void testTablePKSource() throws Exception {
        FlinkTestBase.beforeAll();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a DataStream from the FlussSource
        FlussSource<Order> flussSource =
                FlussSource.<Order>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
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

        env.executeAsync("Test Fluss Orders Source");
        // Wait a bit to collect results in the sink
        TimeUnit.MILLISECONDS.sleep(500);

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

    @Test
    public void testRowDataPKTableSource() throws Exception {
        FlinkTestBase.beforeAll();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Table table = conn.getTable(ordersPKTablePath);
        RowType rowType = table.getTableInfo().getRowType();

        // Create a DataStream from the FlussSource
        FlussSource<RowData> flussSource =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(pkTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new FlussRowDeserializationSchema(rowType))
                        .build();

        DataStreamSource<RowData> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        List<InternalRow> updatedRows =
                Arrays.asList(row(600L, 20L, 800, "addr1"), row(700L, 22L, 801, "addr2"));

        stream.addSink(
                new SinkFunction<RowData>() {
                    @Override
                    public void invoke(RowData value, Context context) {
                        collectedRows.add(value);
                    }
                });
        // send some row updates
        writeRows(ordersPKTablePath, updatedRows, false);
        env.executeAsync("Test Fluss RowData Source");
        // Wait a bit to collect results in the sink
        TimeUnit.MILLISECONDS.sleep(500);

        List<RowData> expectedResult =
                Arrays.asList(
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT),
                        createRowData(800L, 23L, 602, "addr3", RowKind.INSERT),
                        createRowData(900L, 24L, 603, "addr4", RowKind.INSERT),
                        createRowData(1000L, 25L, 604, "addr5", RowKind.INSERT),
                        createRowData(600L, 20L, 600, "addr1", RowKind.UPDATE_BEFORE),
                        createRowData(600L, 20L, 800, "addr1", RowKind.UPDATE_AFTER),
                        createRowData(700L, 22L, 601, "addr2", RowKind.UPDATE_BEFORE),
                        createRowData(700L, 22L, 801, "addr2", RowKind.UPDATE_AFTER));

        // Assert result size and elements match
        Assertions.assertEquals(expectedResult.size(), collectedRows.size());
        Assertions.assertEquals(expectedResult, collectedRows);
    }

    @Test
    public void testRowDataLogTableSource() throws Exception {
        FlinkTestBase.beforeAll();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Table table = conn.getTable(ordersLogTablePath);
        RowType rowType = table.getTableInfo().getRowType();

        // Create a DataStream from the FlussSource
        FlussSource<RowData> flussSource =
                FlussSource.<RowData>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(logTableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new FlussRowDeserializationSchema(rowType))
                        .build();

        DataStreamSource<RowData> stream =
                env.fromSource(flussSource, WatermarkStrategy.noWatermarks(), "Fluss Source");

        // these rows should be interpreted as Inserts
        List<InternalRow> updatedRows =
                Arrays.asList(row(600L, 20L, 600, "addr1"), row(700L, 22L, 601, "addr2"));

        stream.addSink(
                new SinkFunction<RowData>() {
                    @Override
                    public void invoke(RowData value, Context context) {
                        collectedRows.add(value);
                    }
                });

        // send some row updates
        writeRows(ordersLogTablePath, updatedRows, true);
        env.executeAsync("Test Fluss Log RowData Source");
        // Wait a bit to collect results in the sink
        TimeUnit.MILLISECONDS.sleep(500);

        List<RowData> expectedResult =
                Arrays.asList(
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT),
                        createRowData(800L, 23L, 602, "addr3", RowKind.INSERT),
                        createRowData(900L, 24L, 603, "addr4", RowKind.INSERT),
                        createRowData(1000L, 25L, 604, "addr5", RowKind.INSERT),
                        createRowData(600L, 20L, 600, "addr1", RowKind.INSERT),
                        createRowData(700L, 22L, 601, "addr2", RowKind.INSERT));

        // Assert result size and elements match
        Assertions.assertEquals(expectedResult.size(), collectedRows.size());
        Assertions.assertEquals(expectedResult, collectedRows);
    }

    private static RowData createRowData(
            Long orderId, Long itemId, Integer amount, String address, RowKind rowKind) {
        GenericRowData row = new GenericRowData(4);
        row.setField(0, orderId);
        row.setField(1, itemId);
        row.setField(2, amount);
        row.setField(3, StringData.fromString(address));

        row.setRowKind(rowKind);
        return row;
    }

    private void initTables() {
        List<GenericRow> rows =
                orders.stream().map(RowConverters::pojoToGenericRow).collect(Collectors.toList());

        Table pkTable = conn.getTable(ordersPKTablePath);
        Table logTable = conn.getTable(ordersLogTablePath);

        for (GenericRow row : rows) {
            try {
                pkTable.newUpsert().createWriter().upsert(row).get();
                logTable.newAppend().createWriter().append(row).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Failed to insert test data", e);
            }
        }
    }
}
