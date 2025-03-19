/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.serdes;

import com.alibaba.fluss.flink.helper.Order;
import com.alibaba.fluss.flink.helper.OrderDeserializationSchema;
import com.alibaba.fluss.flink.row.FlinkRowToPojo;
import com.alibaba.fluss.flink.row.FlussRecord;
import com.alibaba.fluss.record.ChangeType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlussDeserializationSchemaTest {
    @Test
    public void testDeserialize() throws Exception {
        // Create a mock FlussRecord from a Row object
        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        FlussRecord flussRecord =
                new FlussRecord(0, System.currentTimeMillis(), ChangeType.INSERT, row);

        // Create the deserialization schema
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        // Deserialize the record
        Order order = schema.deserialize(flussRecord);

        // Verify the deserialized object
        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("123 Test St");
    }

    @Test
    public void testGetProducedType() {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();
        TypeInformation<Order> typeInfo = schema.getProducedType();
        assertThat(typeInfo).isEqualTo(TypeInformation.of(Order.class));
    }

    @Test
    public void testConvertPojoToOrder() {
        // Create a Row
        Row row = new Row(5);
        row.setField(0, 1);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        // Convert the row to an Order object
        Order order = FlinkRowToPojo.convert(row, Order.class);

        // Verify the converted object
        assertThat(order.getOrderId()).isEqualTo(1);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("123 Test St");
    }
}
