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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.client.admin.ClientToServerITCaseBase;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Fluss client with POJOs. Tests both writing and reading operations with various
 * scenarios.
 */
public class FlussClientPojoTest extends ClientToServerITCaseBase {

    /** Test writing and reading POJOs with AppendWriter and LogScanner. */
    @Test
    void testAppendAndReadPOJOs() throws Exception {
        // Create a schema for the UserPojo
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        TablePath tablePath = TablePath.of("test_db_1", "test_append_read_pojos");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the AppendWriter and add the RowConverter
            AppendWriter<UserPojo> appendWriter =
                    table.newAppend().createWriter().withRowSerializer(new UserPojoConverter());

            // Create some POJOs to append
            List<UserPojo> users = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                users.add(new UserPojo(i, "User " + i));
            }

            // Append the POJOs
            for (UserPojo user : users) {
                appendWriter.append(user).get();
            }

            // Flush to ensure all data is written
            appendWriter.flush();

            // Create a scanner that directly returns POJOs using the converter
            LogScanner<InternalRow> baseScanner = createLogScanner(table);
            LogScanner<UserPojo> pojoScanner =
                    baseScanner.withRowSerializer(new UserPojoConverter());
            subscribeFromBeginning(pojoScanner, table);

            // Read the POJOs directly from the scanner
            List<UserPojo> readUsers = new ArrayList<>();
            while (readUsers.size() < users.size()) {
                ScanRecords<UserPojo> scanRecords = pojoScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord<UserPojo> scanRecord : scanRecords) {
                    assertThat(scanRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    readUsers.add(scanRecord.getObject());
                }
            }

            // Verify all users were read correctly
            assertThat(readUsers).hasSize(users.size());
            for (int i = 0; i < users.size(); i++) {
                assertThat(readUsers.get(i).getId()).isEqualTo(users.get(i).getId());
                assertThat(readUsers.get(i).getName()).isEqualTo(users.get(i).getName());
            }

            pojoScanner.close();
        }
    }

    /** Test writing and reading POJOs with UpsertWriter and Lookuper. */
    @Test
    void testUpsertAndLookupPOJOs() throws Exception {
        // Create a schema for the UserPojo with a primary key
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        TablePath tablePath = TablePath.of("test_db_1", "test_upsert_lookup_pojos");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the UpsertWriter and add the RowConverter
            UpsertWriter<UserPojo> upsertWriter =
                    table.newUpsert().createWriter().withRowSerializer(new UserPojoConverter());

            // Create some POJOs to upsert
            List<UserPojo> users = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                users.add(new UserPojo(i, "User " + i));
            }

            // Upsert the POJOs
            for (UserPojo user : users) {
                upsertWriter.upsert(user).get();
            }

            // Update some POJOs
            for (int i = 0; i < 5; i++) {
                UserPojo updatedUser = new UserPojo(i, "Updated User " + i);
                upsertWriter.upsert(updatedUser).get();
            }

            // Delete some POJOs
            for (int i = 8; i < 10; i++) {
                upsertWriter.delete(new UserPojo(i, null)).get();
            }

            // Flush to ensure all data is written
            upsertWriter.flush();

            // Get a Lookuper that returns POJOs
            Lookuper<UserPojo> pojoLookuper =
                    table.newLookup().createLookuper().withRowSerializer(new UserPojoConverter());

            // Look up each user and verify the POJO is returned correctly
            // Check updated users (0-4)
            for (int i = 0; i < 5; i++) {
                CompletableFuture<LookupResult<UserPojo>> result = pojoLookuper.lookup(row(i));
                LookupResult<UserPojo> lookupResult = result.get();
                assertThat(lookupResult).isNotNull();

                UserPojo user = lookupResult.getSingletonRow();
                assertThat(user).isNotNull();
                assertThat(user.getId()).isEqualTo(i);
                assertThat(user.getName()).isEqualTo("Updated User " + i);
            }

            // Check non-updated users (5-7)
            for (int i = 5; i < 8; i++) {
                CompletableFuture<LookupResult<UserPojo>> result = pojoLookuper.lookup(row(i));
                LookupResult<UserPojo> lookupResult = result.get();
                assertThat(lookupResult).isNotNull();

                UserPojo user = lookupResult.getSingletonRow();
                assertThat(user).isNotNull();
                assertThat(user.getId()).isEqualTo(i);
                assertThat(user.getName()).isEqualTo("User " + i);
            }

            // Check deleted users (8-9)
            for (int i = 8; i < 10; i++) {
                CompletableFuture<LookupResult<UserPojo>> result = pojoLookuper.lookup(row(i));
                LookupResult<UserPojo> lookupResult = result.get();
                assertThat(lookupResult).isNotNull();
                assertThat(lookupResult.getRowList().isEmpty()).isTrue();
            }
        }
    }

    /** Test with a more complex POJO that has various data types. */
    @Test
    void testComplexPojoWriteAndRead() throws Exception {
        // Create a schema for the ComplexPojo
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("salary", DataTypes.DOUBLE())
                        .column("active", DataTypes.BOOLEAN())
                        .column("hireDate", DataTypes.DATE())
                        .column("bonus", DataTypes.DECIMAL(10, 2))
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        TablePath tablePath = TablePath.of("test_db_1", "test_complex_pojo");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the UpsertWriter and add the RowConverter
            UpsertWriter<ComplexPojo> upsertWriter =
                    table.newUpsert().createWriter().withRowSerializer(new ComplexPojoConverter());

            // Create some complex POJOs
            List<ComplexPojo> employees = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                employees.add(
                        new ComplexPojo(
                                i,
                                "Employee " + i,
                                50000.0 + (i * 10000),
                                i % 2 == 0, // active for even IDs
                                LocalDate.of(2020, 1, 1).plusMonths(i),
                                new BigDecimal("1000.50").add(new BigDecimal(i * 100))));
            }

            // Upsert the POJOs
            for (ComplexPojo employee : employees) {
                upsertWriter.upsert(employee).get();
            }

            // Flush to ensure all data is written
            upsertWriter.flush();

            // Get a Lookuper that returns POJOs
            Lookuper<ComplexPojo> pojoLookuper =
                    table.newLookup()
                            .createLookuper()
                            .withRowSerializer(new ComplexPojoConverter());

            // Look up each employee and verify the POJO is returned correctly
            for (int i = 0; i < employees.size(); i++) {
                CompletableFuture<LookupResult<ComplexPojo>> result = pojoLookuper.lookup(row(i));
                LookupResult<ComplexPojo> lookupResult = result.get();
                assertThat(lookupResult).isNotNull();

                ComplexPojo employee = lookupResult.getSingletonRow();
                assertThat(employee).isNotNull();

                // Verify all fields
                ComplexPojo expected = employees.get(i);
                assertThat(employee.getId()).isEqualTo(expected.getId());
                assertThat(employee.getName()).isEqualTo(expected.getName());
                assertThat(employee.getSalary()).isEqualTo(expected.getSalary());
                assertThat(employee.isActive()).isEqualTo(expected.isActive());
                assertThat(employee.getHireDate()).isEqualTo(expected.getHireDate());
                assertThat(employee.getBonus()).isEqualTo(expected.getBonus());
            }

            // Now test reading with LogScanner
            LogScanner<ComplexPojo> pojoScanner =
                    createLogScanner(table).withRowSerializer(new ComplexPojoConverter());
            subscribeFromBeginning(pojoScanner, table);

            List<ComplexPojo> readEmployees = new ArrayList<>();
            while (readEmployees.size() < employees.size()) {
                ScanRecords<ComplexPojo> scanRecords = pojoScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord<ComplexPojo> scanRecord : scanRecords) {
                    readEmployees.add(scanRecord.getObject());
                }
            }

            // Verify all employees were read correctly
            assertThat(readEmployees).hasSize(employees.size());
            for (int i = 0; i < employees.size(); i++) {
                ComplexPojo actual = readEmployees.get(i);
                ComplexPojo expected = employees.get(i);

                assertThat(actual.getId()).isEqualTo(expected.getId());
                assertThat(actual.getName()).isEqualTo(expected.getName());
                assertThat(actual.getSalary()).isEqualTo(expected.getSalary());
                assertThat(actual.isActive()).isEqualTo(expected.isActive());
                assertThat(actual.getHireDate()).isEqualTo(expected.getHireDate());
                assertThat(actual.getBonus()).isEqualTo(expected.getBonus());
            }

            pojoScanner.close();
        }
    }

    /** Test error handling when trying to use a POJO without a converter. */
    @Test
    void testErrorHandlingWithoutConverter() throws Exception {
        // Create a schema for the UserPojo
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        TablePath tablePath = TablePath.of("test_db_1", "test_error_handling");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the AppendWriter without a converter
            // Use raw type to bypass compile-time type checking
            @SuppressWarnings("rawtypes")
            AppendWriter rawWriter = table.newAppend().createWriter();

            // Try to append a POJO without a converter
            UserPojo user = new UserPojo(1, "Test User");

            // This should throw an exception
            @SuppressWarnings("unchecked")
            final AppendWriter<Object> appendWriter = rawWriter;
            assertThatThrownBy(() -> appendWriter.append(user))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(
                            "Row is not an InternalRow and no RowConverter was provided");
        }
    }

    /** Test handling null values in POJOs. */
    @Test
    void testNullValuesInPOJOs() throws Exception {
        // Create a schema for the UserPojo
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3, "id").build();

        TablePath tablePath = TablePath.of("test_db_1", "test_null_values");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the UpsertWriter and add the RowConverter
            UpsertWriter<UserPojo> upsertWriter =
                    table.newUpsert().createWriter().withRowSerializer(new UserPojoConverter());

            // Create POJOs with null values
            List<UserPojo> users = new ArrayList<>();
            users.add(new UserPojo(1, null)); // null name
            users.add(new UserPojo(2, "User 2")); // non-null name

            // Upsert the POJOs
            for (UserPojo user : users) {
                upsertWriter.upsert(user).get();
            }

            // Flush to ensure all data is written
            upsertWriter.flush();

            // Get a Lookuper that returns POJOs
            Lookuper<UserPojo> pojoLookuper =
                    table.newLookup().createLookuper().withRowSerializer(new UserPojoConverter());

            // Look up the user with null name
            CompletableFuture<LookupResult<UserPojo>> result = pojoLookuper.lookup(row(1));
            LookupResult<UserPojo> lookupResult = result.get();
            assertThat(lookupResult).isNotNull();

            UserPojo user = lookupResult.getSingletonRow();
            assertThat(user).isNotNull();
            assertThat(user.getId()).isEqualTo(1);
            assertThat(user.getName()).isNull();

            // Look up the user with non-null name
            result = pojoLookuper.lookup(row(2));
            lookupResult = result.get();
            assertThat(lookupResult).isNotNull();

            user = lookupResult.getSingletonRow();
            assertThat(user).isNotNull();
            assertThat(user.getId()).isEqualTo(2);
            assertThat(user.getName()).isEqualTo("User 2");
        }
    }

    /** Test batch operations with POJOs. */
    @Test
    void testBatchOperationsWithPOJOs() throws Exception {
        // Create a schema for the UserPojo
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();

        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();

        TablePath tablePath = TablePath.of("test_db_1", "test_batch_operations");
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            // Get the AppendWriter and add the RowConverter
            AppendWriter<UserPojo> appendWriter =
                    table.newAppend().createWriter().withRowSerializer(new UserPojoConverter());

            // Create a large batch of POJOs
            int batchSize = 100;
            List<UserPojo> users = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                users.add(new UserPojo(i, "User " + i));
            }

            // Append all POJOs without waiting for each one
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (UserPojo user : users) {
                futures.add(appendWriter.append(user).thenApply(result -> null));
            }

            // Wait for all operations to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

            // Flush to ensure all data is written
            appendWriter.flush();

            // Create a scanner that directly returns POJOs using the converter
            LogScanner<UserPojo> pojoScanner =
                    createLogScanner(table).withRowSerializer(new UserPojoConverter());
            subscribeFromBeginning(pojoScanner, table);

            // Read the POJOs directly from the scanner
            List<UserPojo> readUsers = new ArrayList<>();
            while (readUsers.size() < batchSize) {
                ScanRecords<UserPojo> scanRecords = pojoScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord<UserPojo> scanRecord : scanRecords) {
                    readUsers.add(scanRecord.getObject());
                }
            }

            // Verify all users were read correctly
            assertThat(readUsers).hasSize(batchSize);
            for (int i = 0; i < batchSize; i++) {
                boolean found = false;
                for (UserPojo user : readUsers) {
                    if (user.getId() == i) {
                        assertThat(user.getName()).isEqualTo("User " + i);
                        found = true;
                        break;
                    }
                }
                assertThat(found).isTrue();
            }

            pojoScanner.close();
        }
    }

    /** A simple POJO class for testing basic operations. */
    static class UserPojo {
        private int id;
        private String name;

        public UserPojo() {
            // Default constructor needed for some frameworks
        }

        public UserPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UserPojo userPojo = (UserPojo) o;
            return id == userPojo.id
                    && (name == null ? userPojo.name == null : name.equals(userPojo.name));
        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "UserPojo{" + "id=" + id + ", name='" + name + '\'' + '}';
        }
    }

    /** A more complex POJO with various data types for testing. */
    static class ComplexPojo {
        private int id;
        private String name;
        private double salary;
        private boolean active;
        private LocalDate hireDate;
        private BigDecimal bonus;

        public ComplexPojo() {
            // Default constructor needed for some frameworks
        }

        public ComplexPojo(
                int id,
                String name,
                double salary,
                boolean active,
                LocalDate hireDate,
                BigDecimal bonus) {
            this.id = id;
            this.name = name;
            this.salary = salary;
            this.active = active;
            this.hireDate = hireDate;
            this.bonus = bonus;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getSalary() {
            return salary;
        }

        public void setSalary(double salary) {
            this.salary = salary;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }

        public LocalDate getHireDate() {
            return hireDate;
        }

        public void setHireDate(LocalDate hireDate) {
            this.hireDate = hireDate;
        }

        public BigDecimal getBonus() {
            return bonus;
        }

        public void setBonus(BigDecimal bonus) {
            this.bonus = bonus;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexPojo that = (ComplexPojo) o;
            return id == that.id
                    && Double.compare(that.salary, salary) == 0
                    && active == that.active
                    && (name == null ? that.name == null : name.equals(that.name))
                    && (hireDate == null ? that.hireDate == null : hireDate.equals(that.hireDate))
                    && (bonus == null ? that.bonus == null : bonus.compareTo(that.bonus) == 0);
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = id;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            temp = Double.doubleToLongBits(salary);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (active ? 1 : 0);
            result = 31 * result + (hireDate != null ? hireDate.hashCode() : 0);
            result = 31 * result + (bonus != null ? bonus.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ComplexPojo{"
                    + "id="
                    + id
                    + ", name='"
                    + name
                    + '\''
                    + ", salary="
                    + salary
                    + ", active="
                    + active
                    + ", hireDate="
                    + hireDate
                    + ", bonus="
                    + bonus
                    + '}';
        }
    }

    /** Converter for UserPojo. */
    static class UserPojoConverter implements RowSerializer<UserPojo> {
        @Override
        public InternalRow toInternalRow(UserPojo pojo) {
            GenericRow row = new GenericRow(2);
            row.setField(0, pojo.getId());
            row.setField(
                    1, pojo.getName() != null ? BinaryString.fromString(pojo.getName()) : null);
            return row;
        }

        @Override
        public UserPojo fromInternalRow(InternalRow row) {
            int id = row.getInt(0);
            String name = row.isNullAt(1) ? null : row.getString(1).toString();
            return new UserPojo(id, name);
        }
    }

    /** Converter for ComplexPojo. */
    static class ComplexPojoConverter implements RowSerializer<ComplexPojo> {
        @Override
        public InternalRow toInternalRow(ComplexPojo pojo) {
            GenericRow row = new GenericRow(6);
            row.setField(0, pojo.getId());
            row.setField(
                    1, pojo.getName() != null ? BinaryString.fromString(pojo.getName()) : null);
            row.setField(2, pojo.getSalary());
            row.setField(3, pojo.isActive());
            // Convert LocalDate to int (days since epoch)
            row.setField(
                    4, pojo.getHireDate() != null ? (int) pojo.getHireDate().toEpochDay() : null);
            // Convert BigDecimal to Decimal
            row.setField(
                    5,
                    pojo.getBonus() != null
                            ? com.alibaba.fluss.row.Decimal.fromBigDecimal(pojo.getBonus(), 10, 2)
                            : null);
            return row;
        }

        @Override
        public ComplexPojo fromInternalRow(InternalRow row) {
            int id = row.getInt(0);
            String name = row.isNullAt(1) ? null : row.getString(1).toString();
            double salary = row.getDouble(2);
            boolean active = row.getBoolean(3);
            LocalDate hireDate = row.isNullAt(4) ? null : LocalDate.ofEpochDay(row.getInt(4));
            BigDecimal bonus = row.isNullAt(5) ? null : row.getDecimal(5, 10, 2).toBigDecimal();
            return new ComplexPojo(id, name, salary, active, hireDate, bonus);
        }
    }
}
