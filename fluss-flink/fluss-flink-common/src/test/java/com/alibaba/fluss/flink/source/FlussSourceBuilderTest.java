package com.alibaba.fluss.flink.source;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.flink.FlussSource;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;

import com.alibaba.fluss.types.RowType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class FlussSourceBuilderTest extends FlinkTestBase {

    private static String bootstrapServers;

    @BeforeEach
    public void setup() throws Exception {
        bootstrapServers = conn.getConfiguration().get(ConfigOptions.BOOTSTRAP_SERVERS).get(0);

        createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);
    }

    @Test
    public void testBuildWithValidConfiguration() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    @Test
    public void testMissingBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("bootstrapServers must not be set");
    }

    @Test
    public void testEmptyBootstrapServers() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers("")
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        assertThatThrownBy(executable::execute)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Failed to initialize FlussSource admin connection: No resolvable bootstrap urls given in bootstrap.servers");
    }

    @Test
    public void testMissingDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database must be set and not empty");
    }

    @Test
    public void testEmptyDatabase() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase("")
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Database must be set and not empty");
    }

    @Test
    public void testMissingTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TableName must be set and not empty");
    }

    @Test
    public void testEmptyTable() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable("")
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("TableName must be set and not empty");
    }

    @Test
    public void testMissingScanPartitionDiscoveryInterval() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("`scanPartitionDiscoveryIntervalMs` must be set and not empty");
    }

    @Test
    public void testMissingOffsetsInitializer() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .setDeserializationSchema(new TestDeserializationSchema())
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("`offsetsInitializer` be set and not empty");
    }

    @Test
    public void testMissingDeserializationSchema() {
        // Given
        Executable executable =
                () ->
                        FlussSource.<TestRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setDatabase(DEFAULT_DB)
                                .setTable(DEFAULT_TABLE_PATH.getTableName())
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setScanPartitionDiscoveryIntervalMs(1000L)
                                .build();

        // Then
        assertThatThrownBy(executable::execute)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("DeserializationSchema must be set");
    }

    @Test
    public void testSetProjectedFields() {
        // Given
        int[] projectedFields = new int[] {0, 1};
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields(projectedFields)
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    @Test
    public void testSetBatchMode() {
        // Given
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setIsBatch(true)
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    @Test
    public void testProjectedFields() {
        // Given
        int[] projectedFields = new int[] {0, 1}; // Only include orderId and amount fields

        // When
        FlussSource<TestRecord> source =
                FlussSource.<TestRecord>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(DEFAULT_TABLE_PATH.getTableName())
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new TestDeserializationSchema())
                        .setProjectedFields(projectedFields)
                        .build();

        // Then
        assertThat(source).isNotNull();
    }

    // Test record class for tests
    private static class TestRecord {
        private int id;
        private String name;

        public TestRecord(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    // Test deserialization schema for tests
    private static class TestDeserializationSchema
            implements FlussDeserializationSchema<TestRecord> {

        @Override
        public void open(InitializationContext context) throws Exception {
        }

        @Override
        public TestRecord deserialize(LogRecord record) throws Exception {
            InternalRow row = record.getRow();
            return new TestRecord(row.getInt(0), row.getString(1).toString());        }

        @Override
        public TypeInformation<TestRecord> getProducedType(RowType rowSchema) {
            return TypeInformation.of(TestRecord.class);
        }
    }
}
