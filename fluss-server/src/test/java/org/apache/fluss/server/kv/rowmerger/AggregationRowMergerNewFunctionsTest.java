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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for newly added aggregation functions in AggregationRowMerger. */
class AggregationRowMergerNewFunctionsTest {

    private static BinaryRow rowOf(KvFormat kv, DataType[] types, Object... values) {
        RowEncoder enc = RowEncoder.create(kv, types);
        enc.startNewRow();
        for (int i = 0; i < values.length; i++) {
            enc.encodeField(i, values[i]);
        }
        return enc.finishRow();
    }

    @Test
    void testNewFunctions() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("s", DataTypes.STRING())       // LISTAGG
                .column("x", DataTypes.INT())          // MIN
                .column("y", DataTypes.DOUBLE())       // MAX
                .column("z", DataTypes.BIGINT())       // PRODUCT
                .column("lv", DataTypes.STRING())      // LAST_VALUE
                .column("fv", DataTypes.STRING())      // FIRST_VALUE
                .column("av", DataTypes.DOUBLE())      // AVG (simplified)
                .primaryKey("id")
                .build();
        RowType rowType = schema.getRowType();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);

        Map<String, AggregationRowMerger.AggFn> fns = new LinkedHashMap<>();
        fns.put("s", AggregationRowMerger.AggFn.LISTAGG);
        fns.put("x", AggregationRowMerger.AggFn.MIN);
        fns.put("y", AggregationRowMerger.AggFn.MAX);
        fns.put("z", AggregationRowMerger.AggFn.PRODUCT);
        fns.put("lv", AggregationRowMerger.AggFn.LAST_VALUE);
        fns.put("fv", AggregationRowMerger.AggFn.FIRST_VALUE);
        fns.put("av", AggregationRowMerger.AggFn.AVG);

        AggregationRowMerger merger = new AggregationRowMerger(schema, KvFormat.COMPACTED, fns);

        BinaryRow oldRow = rowOf(KvFormat.COMPACTED, types,
                10,
                BinaryString.fromString("a"), // s
                5,                             // x
                1.25,                          // y
                3L,                            // z
                BinaryString.fromString("old"),// lv
                BinaryString.fromString("old"),// fv
                2.0                            // av
        );
        BinaryRow newRow = rowOf(KvFormat.COMPACTED, types,
                10,
                BinaryString.fromString("b"), // s
                7,                             // x
                2.5,                           // y
                4L,                            // z
                BinaryString.fromString("new"),// lv
                BinaryString.fromString("new"),// fv
                6.0                            // av
        );

        BinaryRow merged = merger.merge(oldRow, newRow);

        assertThat(merged.getInt(0)).isEqualTo(10);
        assertThat(merged.getString(1).toString()).isEqualTo("a,b"); // listagg
        assertThat(merged.getInt(2)).isEqualTo(5); // min(5,7) = 5
        assertThat(merged.getDouble(3)).isEqualTo(2.5); // max(1.25,2.5) = 2.5
        assertThat(merged.getLong(4)).isEqualTo(12L); // product 3*4 = 12
        assertThat(merged.getString(5).toString()).isEqualTo("new"); // last_value
        assertThat(merged.getString(6).toString()).isEqualTo("old"); // first_value
        assertThat(merged.getDouble(7)).isEqualTo((2.0 + 6.0) / 2.0); // simplified avg
    }

    @Test
    void testNullsInNewFunctions() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("s", DataTypes.STRING())
                .column("x", DataTypes.INT())
                .column("y", DataTypes.DOUBLE())
                .column("z", DataTypes.BIGINT())
                .column("lv", DataTypes.STRING())
                .column("fv", DataTypes.STRING())
                .column("av", DataTypes.DOUBLE())
                .primaryKey("id")
                .build();
        RowType rowType = schema.getRowType();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);

        Map<String, AggregationRowMerger.AggFn> fns = new LinkedHashMap<>();
        fns.put("s", AggregationRowMerger.AggFn.LISTAGG);
        fns.put("x", AggregationRowMerger.AggFn.MIN);
        fns.put("y", AggregationRowMerger.AggFn.MAX);
        fns.put("z", AggregationRowMerger.AggFn.PRODUCT);
        fns.put("lv", AggregationRowMerger.AggFn.LAST_VALUE);
        fns.put("fv", AggregationRowMerger.AggFn.FIRST_VALUE);
        fns.put("av", AggregationRowMerger.AggFn.AVG);

        AggregationRowMerger merger = new AggregationRowMerger(schema, KvFormat.COMPACTED, fns);

        BinaryRow oldRow = rowOf(KvFormat.COMPACTED, types,
                1,
                null,
                null,
                null,
                null,
                BinaryString.fromString("old"),
                null,
                null
        );
        BinaryRow newRow = rowOf(KvFormat.COMPACTED, types,
                1,
                BinaryString.fromString("x"),
                9,
                4.0,
                5L,
                null,
                BinaryString.fromString("new"),
                8.0
        );

        BinaryRow merged = merger.merge(oldRow, newRow);
        assertThat(merged.getString(1).toString()).isEqualTo("x");
        assertThat(merged.getInt(2)).isEqualTo(9);
        assertThat(merged.getDouble(3)).isEqualTo(4.0);
        assertThat(merged.getLong(4)).isEqualTo(5L);
        assertThat(merged.getString(5).toString()).isEqualTo("old");
        assertThat(merged.getString(6).toString()).isEqualTo("new");
        assertThat(merged.getDouble(7)).isEqualTo(8.0);
    }
}
