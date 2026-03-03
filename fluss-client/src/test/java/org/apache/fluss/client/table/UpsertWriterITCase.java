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
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UpsertWriterITCase extends ClientToServerITCaseBase {

    @Test
    public void testUpsertWriterWithArrayType() throws Exception {
        TablePath tablePath = TablePath.of("default", "test_array_upsert");
        Schema schema =
                Schema.newBuilder()
                        .column("doc_id", DataTypes.BIGINT())
                        .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("doc_id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        createTable(tablePath, descriptor, false);

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        assertThat(upsertWriter).isNotNull();

        List<GenericRow> rows =
                Arrays.asList(
                        GenericRow.of(1L, GenericArray.of(0.1f, 0.2f, 0.3f, 0.4f, 0.5f)),
                        GenericRow.of(2L, GenericArray.of(0.6f, 0.7f, 0.8f, 0.9f, 1.0f)),
                        GenericRow.of(3L, GenericArray.of(1.1f, 1.2f, 1.3f, 1.4f, 1.5f)),
                        GenericRow.of(4L, GenericArray.of(1.6f, 1.7f, 1.8f, 1.9f, 2.0f)),
                        GenericRow.of(5L, GenericArray.of(2.1f, 2.2f, 2.3f, 2.4f, 2.5f)));

        for (GenericRow row : rows) {
            upsertWriter.upsert(row);
        }

        upsertWriter.flush();

        // Verify records can be read back
        Lookuper lookuper = table.newLookup().createLookuper();
        for (GenericRow expectedRow : rows) {
            InternalRow lookupKey = GenericRow.of(expectedRow.getField(0));
            InternalRow actualRow = lookuper.lookup(lookupKey).get().getSingletonRow();
            assertThat(actualRow).isNotNull();
            assertThat(actualRow.getLong(0)).isEqualTo(expectedRow.getLong(0));

            InternalArray actualArray = actualRow.getArray(1);
            InternalArray expectedArray = expectedRow.getArray(1);
            assertThat(actualArray.size()).isEqualTo(expectedArray.size());
            for (int i = 0; i < actualArray.size(); i++) {
                assertThat(actualArray.getFloat(i)).isEqualTo(expectedArray.getFloat(i));
            }
        }
    }

    @Test
    public void testUpsertWriterWithRowType() throws Exception {
        TablePath tablePath = TablePath.of("default", "test_row_upsert");
        Schema schema =
                Schema.newBuilder()
                        .column("doc_id", DataTypes.BIGINT())
                        .column("embedding", DataTypes.ROW(DataTypes.FLOAT()))
                        .primaryKey("doc_id")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        createTable(tablePath, descriptor, false);

        Table table = conn.getTable(tablePath);
        UpsertWriter upsertWriter = table.newUpsert().createWriter();
        assertThat(upsertWriter).isNotNull();

        GenericRow expectedRow = GenericRow.of(1L, GenericRow.of(0.1f));
        upsertWriter.upsert(expectedRow);
        upsertWriter.flush();

        // Verify records can be read back
        Lookuper lookuper = table.newLookup().createLookuper();
        InternalRow lookupKey = GenericRow.of(1L);
        InternalRow actualRow = lookuper.lookup(lookupKey).get().getSingletonRow();
        assertThat(actualRow).isNotNull();
        assertThat(actualRow.getLong(0)).isEqualTo(1L);
        assertThat(actualRow.getRow(1, 1).getFloat(0)).isEqualTo(0.1f);
    }
}
