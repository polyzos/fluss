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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Additional tests for AggregationRowMerger unhappy paths and edge cases. */
class AggregationRowMergerAdditionalTest {

    private static BinaryRow rowOf(KvFormat kv, DataType[] types, Object... values) {
        RowEncoder enc = RowEncoder.create(kv, types);
        enc.startNewRow();
        for (int i = 0; i < values.length; i++) {
            enc.encodeField(i, values[i]);
        }
        return enc.finishRow();
    }

    private static Schema schema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("a", DataTypes.INT())
                .column("b", DataTypes.BIGINT())
                .column("c", DataTypes.DOUBLE())
                .primaryKey("id")
                .build();
    }

    @Test
    void testNullHandlingInSumAndCount() {
        Schema schema = schema();
        RowType rowType = schema.getRowType();
        DataType[] types = rowType.getChildren().toArray(new DataType[0]);

        Map<String, AggregationRowMerger.AggFn> fns = new HashMap<>();
        fns.put("a", AggregationRowMerger.AggFn.SUM);
        fns.put("b", AggregationRowMerger.AggFn.COUNT);
        fns.put("c", AggregationRowMerger.AggFn.SUM);

        AggregationRowMerger merger = new AggregationRowMerger(schema, KvFormat.COMPACTED, fns);

        // old has nulls on agg fields, new has some nulls too
        BinaryRow oldRow = rowOf(KvFormat.COMPACTED, types, 1, null, null, null);
        BinaryRow newRow = rowOf(KvFormat.COMPACTED, types, 1, 7, null, 2.5);

        BinaryRow merged = merger.merge(oldRow, newRow);

        assertThat(merged.getInt(0)).isEqualTo(1);
        // a: 0 (old) + 7 (new) = 7
        assertThat(merged.getInt(1)).isEqualTo(7);
        // b: 0 (old) + 1 increment = 1L, stored in BIGINT
        assertThat(merged.getLong(2)).isEqualTo(1L);
        // c: 0.0 (old) + 2.5 (new) = 2.5
        assertThat(merged.getDouble(3)).isEqualTo(2.5);
    }

    @Test
    void testMissingFunctionForNonPkThrows() {
        Schema schema = schema();
        Map<String, AggregationRowMerger.AggFn> fns = new HashMap<>();
        fns.put("a", AggregationRowMerger.AggFn.SUM);
        fns.put("b", AggregationRowMerger.AggFn.COUNT);
        // missing function for column 'c'

        assertThatThrownBy(() -> new AggregationRowMerger(schema, KvFormat.COMPACTED, fns))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be provided for non-primary-key column");
    }

    @Test
    void testFunctionOnPrimaryKeyThrows() {
        Schema schema = schema();
        Map<String, AggregationRowMerger.AggFn> fns = new HashMap<>();
        fns.put("id", AggregationRowMerger.AggFn.SUM);
        fns.put("a", AggregationRowMerger.AggFn.SUM);
        fns.put("b", AggregationRowMerger.AggFn.COUNT);
        fns.put("c", AggregationRowMerger.AggFn.SUM);

        assertThatThrownBy(() -> new AggregationRowMerger(schema, KvFormat.COMPACTED, fns))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be set on primary key column");
    }
}
