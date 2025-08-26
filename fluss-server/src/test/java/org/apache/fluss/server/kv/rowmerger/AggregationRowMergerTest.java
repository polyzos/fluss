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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for AggregationRowMerger. */
class AggregationRowMergerTest {

    private static BinaryRow rowOf(KvFormat kv, DataType[] types, Object... values) {
        RowEncoder enc = RowEncoder.create(kv, types);
        enc.startNewRow();
        for (int i = 0; i < values.length; i++) {
            enc.encodeField(i, values[i]);
        }
        return enc.finishRow();
    }

    @Test
    void testSumAndCountAggregation() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.BIGINT())
                        .column("c", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();
        RowType rowType = schema.getRowType();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);

        Map<String, AggregationRowMerger.AggFn> fns = new HashMap<>();
        fns.put("a", AggregationRowMerger.AggFn.SUM);
        fns.put("b", AggregationRowMerger.AggFn.COUNT);
        fns.put("c", AggregationRowMerger.AggFn.SUM);

        AggregationRowMerger merger = new AggregationRowMerger(schema, KvFormat.COMPACTED, fns);

        BinaryRow oldRow = rowOf(KvFormat.COMPACTED, types, 1, 10, 5L, 1.5);
        BinaryRow newRow = rowOf(KvFormat.COMPACTED, types, 1, 3, null, 2.25);

        BinaryRow merged = merger.merge(oldRow, newRow);

        // id kept from oldRow
        assertThat(merged.getInt(0)).isEqualTo(1);
        // a summed: 10 + 3
        assertThat(merged.getInt(1)).isEqualTo(13);
        // b counted: 5 + 1 -> 6
        assertThat(merged.getLong(2)).isEqualTo(6L);
        // c summed: 1.5 + 2.25
        assertThat(merged.getDouble(3)).isEqualTo(3.75);
    }
}
