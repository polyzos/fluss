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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link FullScanValuesBatchScanner}. */
class FullScanValuesBatchScannerITCase extends ClientToServerITCaseBase {

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

    private static final String DEFAULT_DB = "test-fullscan-client-db";

    @Test
    void testFullScanValues() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "test-fullscan-values");
        long tableId = createTable(tablePath, DEFAULT_TABLE_DESCRIPTOR, true);

        // write rows and collect expected list
        List<InternalRow> expectedAllRows = new ArrayList<>();
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            // upsert 20 rows, bucketed by id
            for (int i = 0; i < 20; i++) {
                InternalRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {i, "v" + i});
                writer.upsert(row);
                expectedAllRows.add(row);
            }
            // flush all
            writer.flush();

            // Now run full-scan for each bucket and verify aggregate equals all expected rows
            List<InternalRow> scannedAllRows = new ArrayList<>();
            for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
                TableBucket tb = new TableBucket(tableId, bucket);
                BatchScanner scanner = table.newScan().createFullScanValuesScanner(tb);
                scannedAllRows.addAll(BatchScanUtils.collectRows(scanner));
                scanner.close();
            }
            assertThat(scannedAllRows).containsExactlyInAnyOrderElementsOf(expectedAllRows);
        }
    }
}
