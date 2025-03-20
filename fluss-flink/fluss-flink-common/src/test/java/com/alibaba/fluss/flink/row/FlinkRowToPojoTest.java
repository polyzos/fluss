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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlinkRowToPojoTest {

    @Test
    public void testBasicConversion() {
        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "123 Test St");

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("123 Test St");
    }

    @Test
    public void testNullValues() {
        Row row = new Row(4);
        row.setField(0, null);
        row.setField(1, null);
        row.setField(2, null);
        row.setField(3, null);

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(0);
        assertThat(order.getItemId()).isEqualTo(0);
        assertThat(order.getAmount()).isEqualTo(0);
        assertThat(order.getAddress()).isNull();
    }

    @Test
    public void testInvalidRowArity() {
        Row row = new Row(3);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);

        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, Order.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Row arity (3) does not match");
    }

    @Test
    public void testPrimitiveTypeConversions() {
        Row row = new Row(4);
        row.setField(0, (byte) 1);
        row.setField(1, (short) 100);
        row.setField(2, (float) 5.7);
        row.setField(3, "Test Address");

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("Test Address");
    }

    @Test
    public void testNumberTypeConversions() {
        Row row = new Row(4);
        row.setField(0, Integer.valueOf(1));
        row.setField(1, Double.valueOf(100.5));
        row.setField(2, Long.valueOf(5));
        row.setField(3, "Test Address");

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(5);
        assertThat(order.getAddress()).isEqualTo("Test Address");
    }

    @Test
    public void testInvalidTypeConversion() {
        Row row = new Row(4);
        row.setField(0, "not a number");
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, "Test Address");

        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, Order.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error converting Flink Row to POJO");
    }

    @Test
    public void testBigNumberConversions() {
        Row row = new Row(4);
        row.setField(0, java.math.BigInteger.valueOf(1));
        row.setField(1, java.math.BigDecimal.valueOf(100));
        row.setField(2, Integer.MAX_VALUE);
        row.setField(3, "Test Address");

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getItemId()).isEqualTo(100L);
        assertThat(order.getAmount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(order.getAddress()).isEqualTo("Test Address");
    }

    @Test
    public void testNonAccessibleClassInstantiation() {
        class InaccessibleOrder {
            private Long orderId;
        }

        Row row = new Row(1);
        row.setField(0, 1L);

        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, InaccessibleOrder.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error converting Flink Row to POJO");
    }

    @Test
    public void testNullRow() {
        assertThatThrownBy(() -> FlinkRowToPojo.convert(null, Order.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error converting Flink Row to POJO");
    }

    @Test
    public void testMaxValues() {
        Row row = new Row(4);
        row.setField(0, Long.MAX_VALUE);
        row.setField(1, Long.MIN_VALUE);
        row.setField(2, Integer.MAX_VALUE);
        row.setField(3, "Test Address");

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getOrderId()).isEqualTo(Long.MAX_VALUE);
        assertThat(order.getItemId()).isEqualTo(Long.MIN_VALUE);
        assertThat(order.getAmount()).isEqualTo(Integer.MAX_VALUE);
        assertThat(order.getAddress()).isEqualTo("Test Address");
    }

    @Test
    public void testMissingNoArgsConstructor() {
        class NoDefaultConstructorOrder {
            private final Long orderId;

            public NoDefaultConstructorOrder(Long orderId) {
                this.orderId = orderId;
            }
        }

        Row row = new Row(1);
        row.setField(0, 1L);

        assertThatThrownBy(() -> FlinkRowToPojo.convert(row, NoDefaultConstructorOrder.class))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Error converting Flink Row to POJO");
    }

    @Test
    public void testBinaryStringToStringConversion() {
        class CustomBinaryString {
            @Override
            public String toString() {
                return "Test Address";
            }

            // Make sure the class name check works
            public String getClassName() {
                return "com.alibaba.fluss.row.BinaryString";
            }
        }

        CustomBinaryString binaryString = new CustomBinaryString();

        Row row = new Row(4);
        row.setField(0, 1L);
        row.setField(1, 100L);
        row.setField(2, 5);
        row.setField(3, binaryString);

        Order order = FlinkRowToPojo.convert(row, Order.class);

        assertThat(order.getAddress()).isEqualTo("Test Address");
    }

    @Test
    public void testBooleanConversions() {
        Row row = new Row(2);
        row.setField(0, 1); // Should convert to true
        row.setField(1, "Test");

        BooleanTestPojo booleanPojo = FlinkRowToPojo.convert(row, BooleanTestPojo.class);
        assertThat(booleanPojo.isActive()).isTrue();

        Row row2 = new Row(2);
        row2.setField(0, 0); // Should convert to false
        row2.setField(1, "Test");

        BooleanTestPojo booleanPojo2 = FlinkRowToPojo.convert(row2, BooleanTestPojo.class);
        assertThat(booleanPojo2.isActive()).isFalse();

        Row row3 = new Row(2);
        row3.setField(0, "true"); // String "true" to boolean
        row3.setField(1, "Test");

        BooleanTestPojo booleanPojo3 = FlinkRowToPojo.convert(row3, BooleanTestPojo.class);
        assertThat(booleanPojo3.isActive()).isTrue();
    }

    @Test
    public void testTimestampConversions() {
        LocalDateTime now = LocalDateTime.now();

        Row row = new Row(3);
        row.setField(0, now);
        row.setField(1, 100L);
        row.setField(2, now.toLocalDate());

        DateTestPojo dateTestPojo = FlinkRowToPojo.convert(row, DateTestPojo.class);

        assertThat(dateTestPojo.getTimestamp()).isNotNull();
        assertThat(dateTestPojo.getTimestamp().getYear()).isEqualTo(now.getYear());
        assertThat(dateTestPojo.getLocalDate()).isNotNull();
    }

    @Test
    public void testStaticFieldsFiltering() {
        Row row = new Row(1);
        row.setField(0, 1L);

        StaticFieldPojo result = FlinkRowToPojo.convert(row, StaticFieldPojo.class);
        assertThat(result.id).isEqualTo(1L);
        assertThat(StaticFieldPojo.STATIC_FIELD).isEqualTo("static");
    }

    @Test
    public void testLocalDateTimeFromLong() {
        Row row = new Row(3);
        long currentTimeMillis = System.currentTimeMillis();
        row.setField(0, currentTimeMillis); // Long to LocalDateTime
        row.setField(1, 100L);
        row.setField(2, LocalDate.now().toString());

        DateTestPojo dateTestPojo = FlinkRowToPojo.convert(row, DateTestPojo.class);
        assertThat(dateTestPojo.getTimestamp()).isNotNull();
    }

    @Test
    public void testDateConversions() {
        Row row = new Row(3);
        LocalDateTime now = LocalDateTime.now();
        row.setField(0, now);
        row.setField(1, 100L);
        row.setField(2, "2023-01-01");

        DateTestPojo dateTestPojo = FlinkRowToPojo.convert(row, DateTestPojo.class);
        assertThat(dateTestPojo.getLocalDate().getYear()).isEqualTo(2023);
    }

    @Test
    public void testUtilDateConversion() {
        Row row = new Row(4);
        row.setField(0, LocalDateTime.now());
        row.setField(1, System.currentTimeMillis());
        row.setField(2, 1L);
        row.setField(3, LocalDate.now().toString());

        UtilDatePojo utilDatePojo = FlinkRowToPojo.convert(row, UtilDatePojo.class);
        assertThat(utilDatePojo.getDateFromDateTime()).isNotNull();
        assertThat(utilDatePojo.getDateFromLong()).isNotNull();
    }

    // Helper test classes
    public static class BooleanTestPojo {
        private boolean active;
        private String name;

        public boolean isActive() {
            return active;
        }

        public String getName() {
            return name;
        }
    }

    public static class DateTestPojo {
        private LocalDateTime timestamp;
        private Long value;
        private LocalDate localDate;

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        public Long getValue() {
            return value;
        }

        public LocalDate getLocalDate() {
            return localDate;
        }
    }

    public static class StaticFieldPojo {
        public static final String STATIC_FIELD = "static";
        private Long id;

        public Long getId() {
            return id;
        }
    }

    public static class UtilDatePojo {
        private Date dateFromDateTime;
        private Date dateFromLong;
        private Long id;
        private String dateStr;

        public Date getDateFromDateTime() {
            return dateFromDateTime;
        }

        public Date getDateFromLong() {
            return dateFromLong;
        }

        public Long getId() {
            return id;
        }

        public String getDateStr() {
            return dateStr;
        }
    }
}
