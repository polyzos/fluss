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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.flink.helper.Order;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlinkRowToPojoTest {

    @Test
    public void testConvert() {
        // Create a Row
        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        // Convert the row to an Order object
        Order order = FlinkRowToPojo.convert(row, Order.class);

        // Verify the converted object
        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("123 Test St");
    }

    @Test
    public void testConvertWithNullValues() {
        // Create a Row with null values
        Row row = new Row(4);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);

        // Convert the row to an Order object
        Order order = FlinkRowToPojo.convert(row, Order.class);

        // Verify the converted object
        assertThat(order.getOrderId()).isEqualTo(0L);
        assertThat(order.getItemId()).isEqualTo(0L);
        assertThat(order.getAmount()).isEqualTo(0);
        assertThat(order.getAddress()).isNull();
    }

    @Test
    public void testConvertWithTypeMismatch() {
        // Create a Row with type mismatch
        Row row = new Row(4);
        row.setField(0, "1");
        row.setField(1, "100");
        row.setField(2, "5");
        row.setField(3, 123);

        // Verify that an exception is thrown
        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, Order.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error converting Flink Row to POJO");
    }

    @Test
    public void testConvertWithExtraFields() {
        // Create a Row with extra fields
        Row row = new Row(5); // Set arity to 5
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");
        row.setField(4, "extra");

        // Verify that an exception is thrown
        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, Order.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Row arity (5) does not match the number of fields (4) in the POJO class");
    }

    @Test
    public void testConvertWithMissingFields() {
        // Create a Row with missing fields
        Row row = new Row(3);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);

        // Verify that an exception is thrown
        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, Order.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Row arity (3) does not match the number of fields (4) in the POJO class");
    }
}
