/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.converter.PojoToRowConverter;
import org.apache.fluss.client.converter.RowToPojoConverter;
import org.apache.fluss.types.RowType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.Upsert;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for writing and reading POJOs via client API. */
public class PojoE2EITCase extends ClientToServerITCaseBase {

    /**
         * Test POJO containing all supported field types used by converters.
         */
        public static class FullPojo {
        // primary key
        public Integer a;
        // all supported converter fields
        public Boolean bool1;
        public Byte tiny;
        public Short small;
        public Integer intv;
        public Long big;
        public Float flt;
        public Double dbl;
        public Character ch; // CHAR(1)
        public String str;   // STRING
        public byte[] bin;   // BINARY
        public byte[] bytes; // BYTES
        public java.math.BigDecimal dec; // DECIMAL(10,2)
        public java.time.LocalDate dt;   // DATE
        public java.time.LocalTime tm;   // TIME_WO_TZ
        public java.time.LocalDateTime tsNtz; // TIMESTAMP_NTZ
        public java.time.Instant tsLtz;       // TIMESTAMP_LTZ

        public FullPojo() {}

        public FullPojo(Integer a,
                        Boolean bool1,
                        Byte tiny,
                        Short small,
                        Integer intv,
                        Long big,
                        Float flt,
                        Double dbl,
                        Character ch,
                        String str,
                        byte[] bin,
                        byte[] bytes,
                        java.math.BigDecimal dec,
                        java.time.LocalDate dt,
                        java.time.LocalTime tm,
                        java.time.LocalDateTime tsNtz,
                        java.time.Instant tsLtz) {
            this.a = a;
            this.bool1 = bool1;
            this.tiny = tiny;
            this.small = small;
            this.intv = intv;
            this.big = big;
            this.flt = flt;
            this.dbl = dbl;
            this.ch = ch;
            this.str = str;
            this.bin = bin;
            this.bytes = bytes;
            this.dec = dec;
            this.dt = dt;
            this.tm = tm;
            this.tsNtz = tsNtz;
            this.tsLtz = tsLtz;
        }
    }

    /**
         * Minimal POJO representing the primary key for {@link FullPojo}.
         */
        public static class FullPojoKey {
        public Integer a;

        public FullPojoKey() {}

        public FullPojoKey(Integer a) { this.a = a; }
    }

    private static Schema fullLogSchema() {
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("bool1", DataTypes.BOOLEAN())
                .column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT())
                .column("intv", DataTypes.INT())
                .column("big", DataTypes.BIGINT())
                .column("flt", DataTypes.FLOAT())
                .column("dbl", DataTypes.DOUBLE())
                .column("ch", DataTypes.CHAR(1))
                .column("str", DataTypes.STRING())
                .column("bin", DataTypes.BINARY(3))
                .column("bytes", DataTypes.BYTES())
                .column("dec", DataTypes.DECIMAL(10, 2))
                .column("dt", DataTypes.DATE())
                .column("tm", DataTypes.TIME())
                .column("tsNtz", DataTypes.TIMESTAMP(3))
                .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                .build();
    }

    private static Schema fullPkSchema() {
        // Same columns as log schema but with PK on 'a'
        return Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("bool1", DataTypes.BOOLEAN())
                .column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT())
                .column("intv", DataTypes.INT())
                .column("big", DataTypes.BIGINT())
                .column("flt", DataTypes.FLOAT())
                .column("dbl", DataTypes.DOUBLE())
                .column("ch", DataTypes.CHAR(1))
                .column("str", DataTypes.STRING())
                .column("bin", DataTypes.BINARY(3))
                .column("bytes", DataTypes.BYTES())
                .column("dec", DataTypes.DECIMAL(10, 2))
                .column("dt", DataTypes.DATE())
                .column("tm", DataTypes.TIME())
                .column("tsNtz", DataTypes.TIMESTAMP(3))
                .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                .primaryKey("a")
                .build();
    }

    private static FullPojo newFullPojo(int i) {
        Integer a = i;
        Boolean bool1 = (i % 2) == 0;
        Byte tiny = (byte) (i - 5);
        Short small = (short) (100 + i);
        Integer intv = 1000 + i;
        Long big = 100000L + i;
        Float flt = 1.5f + i;
        Double dbl = 2.5 + i;
        Character ch = (char) ('a' + (i % 26));
        String str = "s" + i;
        byte[] bin = new byte[] {(byte) i, (byte) (i + 1), (byte) (i + 2)};
        byte[] bytes = new byte[] {(byte) (10 + i), (byte) (20 + i)};
        java.math.BigDecimal dec = new java.math.BigDecimal("12345." + (10 + i)).setScale(2);
        java.time.LocalDate dt = java.time.LocalDate.of(2024, 1, 1).plusDays(i);
        java.time.LocalTime tm = java.time.LocalTime.of(12, (i * 7) % 60, (i * 11) % 60);
        java.time.LocalDateTime tsNtz = java.time.LocalDateTime.of(2024, 1, 1, 0, 0).plusSeconds(i).withNano(0);
        java.time.Instant tsLtz = java.time.Instant.ofEpochMilli(1700000000000L + (i * 1000L));
        return new FullPojo(a, bool1, tiny, small, intv, big, flt, dbl, ch, str, bin, bytes, dec, dt, tm, tsNtz, tsLtz);
    }

    @Test
    void testAppendWriteAndScanPojos() throws Exception {
        // Build all-types log table schema
        Schema schema = Schema.newBuilder()
                .column("a", DataTypes.INT())
                .column("bool1", DataTypes.BOOLEAN())
                .column("tiny", DataTypes.TINYINT())
                .column("small", DataTypes.SMALLINT())
                .column("intv", DataTypes.INT())
                .column("big", DataTypes.BIGINT())
                .column("flt", DataTypes.FLOAT())
                .column("dbl", DataTypes.DOUBLE())
                .column("ch", DataTypes.CHAR(1))
                .column("str", DataTypes.STRING())
                .column("bin", DataTypes.BINARY(3))
                .column("bytes", DataTypes.BYTES())
                .column("dec", DataTypes.DECIMAL(10, 2))
                .column("dt", DataTypes.DATE())
                .column("tm", DataTypes.TIME())
                .column("tsNtz", DataTypes.TIMESTAMP(3))
                .column("tsLtz", DataTypes.TIMESTAMP_LTZ(3))
                .build();
        TablePath path = TablePath.of("pojo_db", "all_types_log");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            // write
            AppendWriter<FullPojo> writer = table.newAppend().createWriter(FullPojo.class);
            List<FullPojo> expected = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                FullPojo u = newFullPojo(i);
                expected.add(u);
                writer.append(u).get();
            }
            writer.flush();

            // read
            Scan scan = table.newScan();
            LogScanner<FullPojo> scanner = scan.createLogScanner(FullPojo.class);
            subscribeFromBeginning(scanner, table);

            List<FullPojo> actual = new ArrayList<>();
            while (actual.size() < expected.size()) {
                ScanRecords<FullPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (ScanRecord<FullPojo> r : recs) {
                    assertThat(r.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    actual.add(r.getValue());
                }
            }
            assertThat(actual).usingRecursiveFieldByFieldElementComparator().containsExactlyInAnyOrderElementsOf(expected);
        }
    }

    @Test
    void testUpsertWriteAndScanPojos() throws Exception {
        // Build all-types PK table schema (PK on 'a')
        Schema schema = fullPkSchema();
        TablePath path = TablePath.of("pojo_db", "all_types_pk");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            Upsert upsert = table.newUpsert();
            UpsertWriter<FullPojo> writer = upsert.createWriter(FullPojo.class);

            FullPojo p1 = newFullPojo(1);
            FullPojo p2 = newFullPojo(2);
            writer.upsert(p1).get();
            writer.upsert(p2).get();

            // update key 1: change a couple of fields
            FullPojo p1Updated = newFullPojo(1);
            p1Updated.str = "a1";
            p1Updated.dec = new java.math.BigDecimal("42.42");
            writer.upsert(p1Updated).get();
            writer.flush();

            // scan as POJOs and verify change types and values
            LogScanner<FullPojo> scanner = table.newScan().createLogScanner(FullPojo.class);
            subscribeFromBeginning(scanner, table);

            List<ChangeType> changes = new ArrayList<>();
            List<FullPojo> values = new ArrayList<>();
            while (values.size() < 4) { // INSERT 1, INSERT 2, UPDATE_BEFORE 1, UPDATE_AFTER 1
                ScanRecords<FullPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (ScanRecord<FullPojo> r : recs) {
                    changes.add(r.getChangeType());
                    values.add(r.getValue());
                }
            }
            assertThat(changes).contains(ChangeType.INSERT, ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER);
            // ensure the last update_after reflects new value
            int lastIdx = changes.lastIndexOf(ChangeType.UPDATE_AFTER);
            assertThat(values.get(lastIdx).str).isEqualTo("a1");
        }
    }

    @Test
    void testPojoLookups() throws Exception {
        Schema schema = fullPkSchema();
        TablePath path = TablePath.of("pojo_db", "lookup_pk");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            UpsertWriter<FullPojo> writer = table.newUpsert().createWriter(FullPojo.class);
            writer.upsert(newFullPojo(1)).get();
            writer.upsert(newFullPojo(2)).get();
            writer.flush();

            // primary key lookup using Lookuper API with POJO key
            Lookuper<FullPojoKey> lookuper = table.newLookup().createLookuper();
            RowType tableSchema = table.getTableInfo().getRowType();
            RowToPojoConverter<FullPojo> rowConv =
                    RowToPojoConverter.of(FullPojo.class, tableSchema, tableSchema);

            LookupResult lr = lookuper.lookup(new FullPojoKey(1)).get();
            FullPojo one = rowConv.fromRow(lr.getSingletonRow());
            assertThat(one.str).isEqualTo("s1");
        }
    }

    @Test
    void testInternalRowLookupAlsoWorks() throws Exception {
        Schema schema = fullPkSchema();
        TablePath path = TablePath.of("pojo_db", "lookup_internalrow");
        TableDescriptor td =
                TableDescriptor.builder().schema(schema).distributedBy(2, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            // write a couple of rows via POJO writer
            UpsertWriter<FullPojo> writer = table.newUpsert().createWriter(FullPojo.class);
            writer.upsert(newFullPojo(101)).get();
            writer.upsert(newFullPojo(202)).get();
            writer.flush();

            // now perform lookup using the raw InternalRow path to ensure it's still supported
            Lookuper<InternalRow> lookuper = table.newLookup().createLookuper();
            RowType tableSchema = table.getTableInfo().getRowType();
            RowType keyProjection = tableSchema.project(table.getTableInfo().getPrimaryKeys());

            // Build the key row directly using GenericRow to avoid any POJO conversion
            GenericRow keyRow = new GenericRow(keyProjection.getFieldCount());
            keyRow.setField(0, 101); // primary key field 'a'

            LookupResult lr = lookuper.lookup(keyRow).get();
            RowToPojoConverter<FullPojo> rowConv =
                    RowToPojoConverter.of(FullPojo.class, tableSchema, tableSchema);
            FullPojo pojo = rowConv.fromRow(lr.getSingletonRow());
            assertThat(pojo).isNotNull();
            assertThat(pojo.a).isEqualTo(101);
            assertThat(pojo.str).isEqualTo("s101");
        }
    }

    @Test
    void testProjectionsWithPojos() throws Exception {
        TablePath path = TablePath.of("pojo_db", "proj_log");
        TableDescriptor td = TableDescriptor.builder().schema(fullLogSchema()).distributedBy(1).build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            AppendWriter<FullPojo> writer = table.newAppend().createWriter(FullPojo.class);
            writer.append(newFullPojo(10)).get();
            writer.append(newFullPojo(11)).get();
            writer.flush();

            // Project only a subset of fields
            LogScanner<FullPojo> scanner = table.newScan().project(Arrays.asList("a", "str")).createLogScanner(FullPojo.class);
            subscribeFromBeginning(scanner, table);
            ScanRecords<FullPojo> recs = scanner.poll(Duration.ofSeconds(2));
            for (ScanRecord<FullPojo> r : recs) {
                FullPojo u = r.getValue();
                assertThat(u.a).isNotNull();
                assertThat(u.str).isNotNull();
                // non-projected fields should be null
                assertThat(u.bool1).isNull();
                assertThat(u.bin).isNull();
                assertThat(u.bytes).isNull();
                assertThat(u.dec).isNull();
                assertThat(u.dt).isNull();
                assertThat(u.tm).isNull();
                assertThat(u.tsNtz).isNull();
                assertThat(u.tsLtz).isNull();
            }
        }
    }

    @Test
    void testPartialUpdatesWithPojos() throws Exception {
        // Use full PK schema and update a subset of fields
        Schema schema = fullPkSchema();
        TablePath path = TablePath.of("pojo_db", "pk_partial");
        TableDescriptor td = TableDescriptor.builder().schema(schema).distributedBy(1, "a").build();
        createTable(path, td, true);

        try (Table table = conn.getTable(path)) {
            Upsert upsert = table.newUpsert().partialUpdate("a", "str", "dec");
            UpsertWriter<FullPojo> writer = upsert.createWriter(FullPojo.class);

            // initial full row
            writer.upsert(newFullPojo(1)).get();

            // partial update: only PK + subset fields
            FullPojo patch = new FullPojo();
            patch.a = 1;
            patch.str = "second";
            patch.dec = new java.math.BigDecimal("99.99");
            writer.upsert(patch).get();
            writer.flush();

            // verify via lookup and scan using Lookuper + POJO key
            Lookuper<FullPojoKey> lookuper = table.newLookup().createLookuper();
            RowType tableSchema = table.getTableInfo().getRowType();
            RowToPojoConverter<FullPojo> rowConv =
                    RowToPojoConverter.of(FullPojo.class, tableSchema, tableSchema);
            FullPojo lookedUp = rowConv.fromRow(
                    lookuper.lookup(new FullPojoKey(1)).get().getSingletonRow());
            assertThat(lookedUp.str).isEqualTo("second");
            assertThat(lookedUp.dec).isEqualByComparingTo("99.99");

            LogScanner<FullPojo> scanner = table.newScan().createLogScanner(FullPojo.class);
            subscribeFromBeginning(scanner, table);
            boolean sawUpdateAfter = false;
            while (!sawUpdateAfter) {
                ScanRecords<FullPojo> recs = scanner.poll(Duration.ofSeconds(2));
                for (ScanRecord<FullPojo> r : recs) {
                    if (r.getChangeType() == ChangeType.UPDATE_AFTER) {
                        assertThat(r.getValue().str).isEqualTo("second");
                        sawUpdateAfter = true;
                    }
                }
            }
        }
    }
}
