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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.UserCodeClassLoader;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/**
 * Test class for the {@link RowDataDeserializationSchema} that validates the conversion from Fluss
 * {@link com.alibaba.fluss.record.LogRecord} to Flink's {@link RowData} format.
 */
public class RowDataDeserializationSchemaTest {

    private RowType rowType;
    private RowDataDeserializationSchema schema;

    @BeforeEach
    public void setUp() throws Exception {
        List<DataField> fields =
                Arrays.asList(
                        new DataField("orderId", DataTypes.BIGINT()),
                        new DataField("itemId", DataTypes.BIGINT()),
                        new DataField("amount", DataTypes.INT()),
                        new DataField("address", DataTypes.STRING()));

        rowType = new RowType(fields);
        schema = getRowDataDeserializationSchema(rowType);
    }

    @Test
    public void testDeserialize() throws Exception {
        // Create test data
        GenericRow row = new GenericRow(4);
        row.setField(0, 100L);
        row.setField(1, 10L);
        row.setField(2, 45);
        row.setField(3, BinaryString.fromString("Test addr"));

        ScanRecord scanRecord = new ScanRecord(row);

        RowDataDeserializationSchema deserializer = getRowDataDeserializationSchema(rowType);
        RowData result = deserializer.deserialize(scanRecord);

        assertThat(result.getArity()).isEqualTo(4);
        assertThat(result.getLong(0)).isEqualTo(100L);
        assertThat(result.getLong(1)).isEqualTo(10L);
        assertThat(result.getInt(2)).isEqualTo(45);
        assertThat(result.getString(3).toString()).isEqualTo("Test addr");
    }

    private @NotNull RowDataDeserializationSchema getRowDataDeserializationSchema(RowType rowType)
            throws Exception {
        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(
                new FlussDeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return null;
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return null;
                    }

                    @Override
                    public RowType getRowSchema() {
                        return rowType;
                    }
                });
        return deserializationSchema;
    }

    @Test
    public void testDeserializeWithNullRecord() {
        assertThrows(NullPointerException.class, () -> schema.deserialize(null));
    }

    @Test
    public void testGetProducedType() {
        TypeInformation<RowData> typeInfo = schema.getProducedType();

        assertNotNull(typeInfo);
        assertEquals(RowData.class, typeInfo.getTypeClass());
        assertThat(typeInfo.createSerializer(new ExecutionConfig()))
                .isInstanceOf(RowDataSerializer.class);
    }

    @Test
    public void testSerializable() throws Exception {
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(schema);
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        RowDataDeserializationSchema deserializedSchema =
                (RowDataDeserializationSchema) ois.readObject();
        ois.close();

        // Verify
        assertNotNull(deserializedSchema);
        assertNotNull(deserializedSchema.getProducedType());
        assertEquals(schema.getProducedType(), deserializedSchema.getProducedType());
        assertThat(schema.getProducedType().createSerializer(new ExecutionConfig()))
                .isInstanceOf(RowDataSerializer.class);
    }

    @Test
    public void testDifferentRowTypes() throws Exception {
        // Test with different row types
        List<DataField> simpleFields = Arrays.asList(new DataField("id", DataTypes.BIGINT()));

        RowDataDeserializationSchema simpleSchema =
                getRowDataDeserializationSchema(new RowType(simpleFields));

        assertNotNull(simpleSchema);
        assertEquals(RowData.class, simpleSchema.getProducedType().getTypeClass());
    }
}
