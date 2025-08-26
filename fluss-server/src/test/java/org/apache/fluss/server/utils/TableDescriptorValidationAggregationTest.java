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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests validation rules for aggregation merge engine options. */
class TableDescriptorValidationAggregationTest {

    private static TableDescriptor.Builder baseDescriptorBuilder(Schema schema) {
        Map<String, String> props = new HashMap<>();
        // minimal required table properties
        props.put(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "1");
        props.put(ConfigOptions.TABLE_LOG_FORMAT.key(), LogFormat.ARROW.name());
        props.put(ConfigOptions.TABLE_KV_FORMAT.key(), KvFormat.COMPACTED.name());
        props.put(ConfigOptions.TABLE_MERGE_ENGINE.key(), MergeEngineType.AGGREGATION.name());
        return TableDescriptor.builder().schema(schema).properties(props).distributedBy(1, "id");
    }

    @Test
    void testPerFieldUnknownColumnFails() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("a", DataTypes.INT())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        b.property("fields.b.aggregation-function", "sum"); // unknown column 'b'

        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("unknown column 'b'");
    }

    @Test
    void testWrongTypeForSumFails() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("s", DataTypes.STRING())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        b.property("fields.s.aggregation-function", "sum");

        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("SUM is only supported");
    }

    @Test
    void testWrongTypeForCountFails() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("x", DataTypes.DOUBLE())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        b.property("fields.x.aggregation-function", "count");

        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("COUNT is only supported");
    }

    @Test
    void testLegacyCombinedOptionValidation() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("a", DataTypes.INT())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        // invalid entry format in legacy combined option
        b.property(ConfigOptions.TABLE_MERGE_ENGINE_AGGREGATION_FUNCTIONS.key(), "a-sum");

        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Expected format 'col:func'");
    }

    @Test
    void testLegacyCombinedWrongType() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("s", DataTypes.STRING())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        // SUM on string should fail
        b.property(ConfigOptions.TABLE_MERGE_ENGINE_AGGREGATION_FUNCTIONS.key(), "s:sum");

        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("SUM is only supported");
    }
    @Test
    void testListAggOnNonStringFails() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("s", DataTypes.INT())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        b.property("fields.s.aggregation-function", "listagg");
        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("LISTAGG is only supported for STRING");
    }

    @Test
    void testMinOnNonNumericFails() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("t", DataTypes.STRING())
                .primaryKey("id")
                .build();
        TableDescriptor.Builder b = baseDescriptorBuilder(schema);
        b.property("fields.t.aggregation-function", "min");
        assertThatThrownBy(() -> TableDescriptorValidation.validateTableDescriptor(b.build(), 16))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("MIN is only supported for numeric");
    }
}
