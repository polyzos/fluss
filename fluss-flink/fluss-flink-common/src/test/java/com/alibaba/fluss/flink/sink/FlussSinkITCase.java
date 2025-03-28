package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.source.deserializer.Order;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.flink.source.testutils.MockDataUtils;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;

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
import java.util.List;
import java.util.stream.Collectors;

public class FlussSinkITCase extends FlinkTestBase {
    private static final Schema pkSchema = MockDataUtils.getOrdersSchemaPK();

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
        DataStream<Order> stream = env.fromData(MockDataUtils.ORDERS);

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
                        .setTablePath(DEFAULT_DB, pkTableName)
                        .useUpsert()
                        .setRowType(rowType)
                        .setPojoClass(Order.class) // Signals that no conversion is needed
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
        Assertions.assertEquals(expectedResult.size(), MockDataUtils.ORDERS.size());
        Assertions.assertEquals(expectedResult, MockDataUtils.ORDERS);
    }
}
