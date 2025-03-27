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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/**
 * Test class for the {@link RowDataDeserializationSchema} that validates the conversion from Fluss
 * {@link com.alibaba.fluss.record.LogRecord} to Flink's {@link RowData} format.
 */
public class FlussRowDeserializerTest {

    private RowType rowType;
    private RowDataDeserializationSchema schema;

    @BeforeEach
    public void setUp() {
        // Create a sample row type with different data types
        List<DataField> fields =
                Arrays.asList(
                        new DataField("orderId", DataTypes.BIGINT()),
                        new DataField("itemId", DataTypes.BIGINT()),
                        new DataField("amount", DataTypes.INT()),
                        new DataField("address", DataTypes.STRING()));
        rowType = new RowType(fields);
        schema = new RowDataDeserializationSchema(rowType);
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

        RowDataDeserializationSchema testSchema = new RowDataDeserializationSchema(rowType);

        RowData result = testSchema.deserialize(scanRecord);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(row, scanRecord.getRow());
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
    }

    @Test
    public void testDifferentRowTypes() {
        // Test with different row types
        List<DataField> simpleFields = Arrays.asList(new DataField("id", DataTypes.BIGINT()));
        RowType simpleRowType = new RowType(simpleFields);

        RowDataDeserializationSchema simpleSchema = new RowDataDeserializationSchema(simpleRowType);

        assertNotNull(simpleSchema);
        assertEquals(RowData.class, simpleSchema.getProducedType().getTypeClass());
    }
}
