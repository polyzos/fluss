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

package org.apache.fluss.client.lookup.poller;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for {@link FullTablePoller}. */
class FullTableValuesPollerITCase extends ClientToServerITCaseBase {

    private static final int DEFAULT_BUCKET_NUM = 3;

    private static final Schema DEFAULT_SCHEMA =
            Schema.newBuilder()
                    .primaryKey("id")
                    .column("id", DataTypes.INT())
                    .column("name", DataTypes.STRING())
                    .build();

    private static final TableDescriptor DEFAULT_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(DEFAULT_SCHEMA)
                    .distributedBy(DEFAULT_BUCKET_NUM, "id")
                    .build();

    private static final String DEFAULT_DB = "test-fulltable-poller-db";

    @Test
    void testPeriodicSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-poller-values");
        createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        List<InternalRow> expectedAllRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (int i = 0; i < 12; i++) {
                InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
                writer.upsert(row);
                expectedAllRows.add(row);
            }
            writer.flush();

            FullTablePoller poller = table.newLookup().createFullTableValuesPoller(Duration.ofMillis(200));
            CompletableFuture<List<InternalRow>> first = new CompletableFuture<>();
            poller.subscribe(first::complete);
            poller.start();
            List<InternalRow> snapshot = first.get(10, TimeUnit.SECONDS);
            poller.close();

            assertThat(snapshot).containsExactlyInAnyOrderElementsOf(expectedAllRows);
        }
    }
}
