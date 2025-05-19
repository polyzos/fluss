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

import com.alibaba.fluss.flink.source.testutils.Order;
import com.alibaba.fluss.metadata.DataLakeFormat;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussSinkBuilder} configuration and argument handling. */
class FlussSinkBuilderTest {
    private String BOOTSTRAP_SERVERS = "localhost:9123";
    private String DATABASE_NAME = "testDb";
    private String TABLE_NAME = "testTable";

    private FlussSinkBuilder<Order> builder;
    private RowType orderRowType;

    @BeforeEach
    void setUp() {
        builder = new FlussSinkBuilder<>();

        // Define row type for Order class
        orderRowType =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(false), // id
                            new BigIntType(), // age
                            new IntType(),
                            new VarCharType(true, VarCharType.MAX_LENGTH)
                        },
                        new String[] {"orderId", "itemId", "amount", "address"});
    }

    @Test
    void testConfigurationValidation() throws Exception {
        // Test missing bootstrap servers
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setDatabase("testDb")
                                        .setTable("testTable")
                                        .setRowType(orderRowType)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("BootstrapServers is required but not provided.");

        // Test missing database
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                                        .setTable(TABLE_NAME)
                                        .setRowType(orderRowType)
                                        .build())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database is required but not provided.");

        // Test empty database
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                                        .setDatabase("")
                                        .setTable(TABLE_NAME)
                                        .setRowType(orderRowType)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Database cannot be empty");

        // Test missing table name
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                                        .setDatabase("testDb")
                                        .setRowType(orderRowType)
                                        .build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Table name is required");

        // Test empty table name
        assertThatThrownBy(
                        () ->
                                new FlussSinkBuilder<Order>()
                                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                                        .setDatabase("testDb")
                                        .setTable("")
                                        .setRowType(orderRowType)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Table name cannot be empty");
    }

    @Test
    void testTablePathSetting() throws Exception {
        // Using setDatabase and setTable
        builder.setBootstrapServers(BOOTSTRAP_SERVERS)
                .setDatabase(DATABASE_NAME)
                .setTable(TABLE_NAME)
                .setRowType(orderRowType);

        String database = getFieldValue(builder, "database");
        String tableName = getFieldValue(builder, "tableName");

        assertThat(database).isEqualTo(DATABASE_NAME);
        assertThat(tableName).isEqualTo(TABLE_NAME);
    }

    @Test
    void testConfigOptions() throws Exception {
        builder.setOption("custom.key", "custom.value");
        Map<String, String> configOptions = getFieldValue(builder, "configOptions");
        assertThat(configOptions).containsEntry("custom.key", "custom.value");

        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("option1", "value1");
        optionsMap.put("option2", "value2");

        builder.setOptions(optionsMap);
        configOptions = getFieldValue(builder, "configOptions");

        assertThat(configOptions)
                .containsEntry("custom.key", "custom.value")
                .containsEntry("option1", "value1")
                .containsEntry("option2", "value2");
    }

    @Test
    void testUpsertAndAppendModes() throws Exception {
        // Test upsert mode
        builder.useUpsert();
        boolean isUpsert = getFieldValue(builder, "isUpsert");
        assertThat(isUpsert).isTrue();

        // Test append mode
        builder.useAppend();
        isUpsert = getFieldValue(builder, "isUpsert");
        assertThat(isUpsert).isFalse();
    }

    @Test
    void testIgnoreDelete() throws Exception {
        // Default should be false
        boolean ignoreDelete = getFieldValue(builder, "ignoreDelete");
        assertThat(ignoreDelete).isFalse();

        // Test setting to true
        builder.setIgnoreDelete(true);
        ignoreDelete = getFieldValue(builder, "ignoreDelete");
        assertThat(ignoreDelete).isTrue();
    }

    @Test
    void testDataLakeFormat() throws Exception {
        // Default should be null
        DataLakeFormat lakeFormat = getFieldValue(builder, "lakeFormat");
        assertThat(lakeFormat).isNull();

        // Test setting format
        builder.setDataLakeFormat(DataLakeFormat.PAIMON);
        lakeFormat = getFieldValue(builder, "lakeFormat");
        assertThat(lakeFormat).isEqualTo(DataLakeFormat.PAIMON);
    }

    @Test
    void testShuffleByBucketId() throws Exception {
        // Default should be true
        boolean shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isTrue();

        // Test setting to false
        builder.setShuffleByBucketId(false);
        shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isFalse();

        // Test setting back to true should not change value (implementation detail)
        builder.setShuffleByBucketId(true);
        shuffleByBucketId = getFieldValue(builder, "shuffleByBucketId");
        assertThat(shuffleByBucketId).isFalse();
    }

    @Test
    void testTargetColumnIndexes() throws Exception {
        // Default should be null
        int[] targetColumnIndexes = getFieldValue(builder, "targetColumnIndexes");
        assertThat(targetColumnIndexes).isNull();

        // Test setting indexes
        int[] indexes = {0, 2, 3};
        builder.setTargetColumnIndexes(indexes);
        targetColumnIndexes = getFieldValue(builder, "targetColumnIndexes");
        assertThat(targetColumnIndexes).isEqualTo(indexes);
    }

    @Test
    void testRowTypeSettings() throws Exception {
        // Default should be null
        RowType tableRowType = getFieldValue(builder, "tableRowType");
        assertThat(tableRowType).isNull();

        // Test setting row type
        builder.setRowType(orderRowType);
        tableRowType = getFieldValue(builder, "tableRowType");
        assertThat(tableRowType).isEqualTo(orderRowType);
    }

    @Test
    void testBootstrapServersSetting() throws Exception {
        // Default should be null
        String bootstrapServers = getFieldValue(builder, "bootstrapServers");
        assertThat(bootstrapServers).isNull();

        // Test setting bootstrap servers
        builder.setBootstrapServers(BOOTSTRAP_SERVERS);
        bootstrapServers = getFieldValue(builder, "bootstrapServers");
        assertThat(bootstrapServers).isEqualTo(BOOTSTRAP_SERVERS);
    }

    @Test
    void testFluentChaining() {
        // Test that all methods can be chained
        FlussSinkBuilder<Order> chainedBuilder =
                new FlussSinkBuilder<Order>()
                        .setBootstrapServers(BOOTSTRAP_SERVERS)
                        .setDatabase(DATABASE_NAME)
                        .setTable(TABLE_NAME)
                        .setRowType(orderRowType)
                        .setIgnoreDelete(true)
                        .setTargetColumnIndexes(new int[] {0, 1})
                        .useUpsert()
                        .setOption("key1", "value1")
                        .setOptions(new HashMap<>())
                        .setDataLakeFormat(DataLakeFormat.PAIMON)
                        .setShuffleByBucketId(false);

        // Verify the builder instance is returned
        assertThat(chainedBuilder).isInstanceOf(FlussSinkBuilder.class);
    }

    // Helper method to get private field values using reflection
    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(Object object, String fieldName) throws Exception {
        Field field = FlussSinkBuilder.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(object);
    }
}
