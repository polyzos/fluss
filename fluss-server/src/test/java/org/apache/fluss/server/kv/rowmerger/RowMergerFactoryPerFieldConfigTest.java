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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests RowMerger factory using per-field aggregation properties. */
class RowMergerFactoryPerFieldConfigTest {

    @Test
    void testCreateAggregationMergerFromPerFieldOptions() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("a", DataTypes.INT())
                .column("b", DataTypes.BIGINT())
                .primaryKey("id")
                .build();
        RowType rowType = schema.getRowType();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);

        // Simulate Flink SQL DDL WITH (...) by filling Configuration directly
        Configuration raw = new Configuration();
        raw.setString("table.merge-engine", MergeEngineType.AGGREGATION.name());
        raw.setString("table.kv-format", KvFormat.COMPACTED.name());
        raw.setString("fields.a.aggregation-function", "sum");
        raw.setString("fields.b.aggregation-function", "count");
        TableConfig tableConfig = new TableConfig(raw);

        RowMerger merger = RowMerger.create(tableConfig, schema, KvFormat.COMPACTED);
        assertThat(merger).isInstanceOf(AggregationRowMerger.class);

        // Validate it actually aggregates
        RowEncoder enc = RowEncoder.create(KvFormat.COMPACTED, types);
        enc.startNewRow(); enc.encodeField(0, 7); enc.encodeField(1, 10); enc.encodeField(2, 3L);
        BinaryRow oldRow = enc.finishRow();
        enc.startNewRow(); enc.encodeField(0, 7); enc.encodeField(1, 5); enc.encodeField(2, null);
        BinaryRow newRow = enc.finishRow();

        BinaryRow merged = merger.merge(oldRow, newRow);
        assertThat(merged.getInt(0)).isEqualTo(7);
        assertThat(merged.getInt(1)).isEqualTo(15); // sum
        assertThat(merged.getLong(2)).isEqualTo(4L); // count increments by 1
    }

    // Example Flink SQL DDL (illustrative):
    // CREATE TABLE t (
    //   id INT NOT NULL,
    //   a INT,
    //   b BIGINT,
    //   PRIMARY KEY (id) NOT ENFORCED
    // ) WITH (
    //   'table.merge-engine' = 'AGGREGATION',
    //   'fields.a.aggregation-function' = 'sum',
    //   'fields.b.aggregation-function' = 'count'
    // );
}
