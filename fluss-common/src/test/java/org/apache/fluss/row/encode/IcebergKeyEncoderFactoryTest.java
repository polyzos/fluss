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

package org.apache.fluss.row.encode;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.decode.KeyDecoder;
import org.apache.fluss.row.encode.iceberg.IcebergKeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the key encoder factories of {@link KeyEncoder} and {@link KeyDecoder} on Iceberg
 * tables with a multi-column bucket key.
 */
class IcebergKeyEncoderFactoryTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.STRING(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.INT()
                    },
                    new String[] {"a", "b", "c", "v"});

    private static final List<String> PRIMARY_KEY = Arrays.asList("a", "b", "c");
    private static final List<String> BUCKET_KEY = Arrays.asList("a", "b");

    @Test
    void testBucketKeyEncoderHashesFirstBucketKeyColumnOnly() {
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(ROW_TYPE, BUCKET_KEY, DataLakeFormat.ICEBERG);
        IcebergKeyEncoder firstColumnEncoder =
                new IcebergKeyEncoder(ROW_TYPE, Collections.singletonList("a"));

        InternalRow row1 = row(BinaryString.fromString("x"), BinaryString.fromString("y"), 1L, 0);
        InternalRow row2 = row(BinaryString.fromString("x"), BinaryString.fromString("z"), 2L, 0);
        assertThat(bucketKeyEncoder.encodeKey(row1)).isEqualTo(firstColumnEncoder.encodeKey(row1));
        // rows sharing the first bucket key column always land in the same bucket
        assertThat(bucketKeyEncoder.encodeKey(row2)).isEqualTo(bucketKeyEncoder.encodeKey(row1));
    }

    @Test
    void testSingleColumnBucketKeyEncodingIsUnchanged() {
        List<String> bucketKey = Collections.singletonList("c");
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(ROW_TYPE, bucketKey, DataLakeFormat.ICEBERG);
        InternalRow row = row(BinaryString.fromString("x"), BinaryString.fromString("y"), 7L, 0);
        assertThat(bucketKeyEncoder.encodeKey(row))
                .isEqualTo(new IcebergKeyEncoder(ROW_TYPE, bucketKey).encodeKey(row));
    }

    @Test
    void testPrimaryAndPrefixKeyEncodersAreCompactedAndPrefixCompatible() {
        TableConfig tableConfig = icebergTableConfig(2);
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, PRIMARY_KEY, tableConfig, false);
        // the prefix lookup key encoder is built the same way as in PrefixKeyLookuper
        KeyEncoder prefixKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, BUCKET_KEY, tableConfig, false);
        assertThat(primaryKeyEncoder).isInstanceOf(CompactedKeyEncoder.class);
        assertThat(prefixKeyEncoder).isInstanceOf(CompactedKeyEncoder.class);

        InternalRow row = row(BinaryString.fromString("x"), BinaryString.fromString("y"), 1L, 0);
        assertThat(primaryKeyEncoder.encodeKey(row)).startsWith(prefixKeyEncoder.encodeKey(row));
    }

    @Test
    void testCompositePrimaryKeysDoNotCollide() {
        // The Iceberg key encoding writes strings without a length prefix, so it must never encode
        // a composite primary key: ("ab", "c") and ("a", "bc") would produce the same bytes.
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, PRIMARY_KEY, icebergTableConfig(2), false);
        InternalRow row1 = row(BinaryString.fromString("ab"), BinaryString.fromString("c"), 1L, 0);
        InternalRow row2 = row(BinaryString.fromString("a"), BinaryString.fromString("bc"), 1L, 0);
        assertThat(primaryKeyEncoder.encodeKey(row1))
                .isNotEqualTo(primaryKeyEncoder.encodeKey(row2));

        assertThatThrownBy(() -> new IcebergKeyEncoder(ROW_TYPE, PRIMARY_KEY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Key fields must have exactly one field");
    }

    @Test
    void testPrimaryKeyDecoderRoundTrip() {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, PRIMARY_KEY, icebergTableConfig(2), false);
        KeyDecoder primaryKeyDecoder =
                KeyDecoder.ofPrimaryKeyDecoder(
                        ROW_TYPE, PRIMARY_KEY, (short) 2, DataLakeFormat.ICEBERG, false);

        InternalRow row = row(BinaryString.fromString("x"), BinaryString.fromString("y"), 9L, 0);
        InternalRow decoded = primaryKeyDecoder.decodeKey(primaryKeyEncoder.encodeKey(row));
        assertThat(decoded.getString(0).toString()).isEqualTo("x");
        assertThat(decoded.getString(1).toString()).isEqualTo("y");
        assertThat(decoded.getLong(2)).isEqualTo(9L);
    }

    @Test
    void testBucketKeyEncoderReusesPrimaryKeyEncoderForSingleColumnDefaultKey() {
        List<String> key = Collections.singletonList("c");
        TableConfig tableConfig = icebergTableConfig(2);
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, key, tableConfig, true);
        assertThat(primaryKeyEncoder).isInstanceOf(IcebergKeyEncoder.class);
        assertThat(
                        KeyEncoder.ofBucketKeyEncoder(
                                ROW_TYPE, key, tableConfig, true, primaryKeyEncoder))
                .isSameAs(primaryKeyEncoder);
    }

    @Test
    void testCompositeDefaultBucketKeyIsRejected() {
        // a composite default bucket key is rejected at table creation; the factory guards the
        // invariant that allows reusing the primary key encoder for bucketing
        KeyEncoder primaryKeyEncoder = CompactedKeyEncoder.createKeyEncoder(ROW_TYPE, PRIMARY_KEY);
        assertThatThrownBy(
                        () ->
                                KeyEncoder.ofBucketKeyEncoder(
                                        ROW_TYPE,
                                        PRIMARY_KEY,
                                        icebergTableConfig(2),
                                        true,
                                        primaryKeyEncoder))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must have exactly one column");
        // without a lake format, the reuse is still allowed for composite keys
        assertThat(
                        KeyEncoder.ofBucketKeyEncoder(
                                ROW_TYPE,
                                PRIMARY_KEY,
                                new TableConfig(new Configuration()),
                                true,
                                primaryKeyEncoder))
                .isSameAs(primaryKeyEncoder);
    }

    @Test
    void testNonDefaultBucketKeyEncoderOnIcebergHashesFirstColumn() {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(ROW_TYPE, PRIMARY_KEY, icebergTableConfig(2), false);
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        ROW_TYPE, BUCKET_KEY, icebergTableConfig(2), false, primaryKeyEncoder);
        InternalRow row = row(BinaryString.fromString("x"), BinaryString.fromString("y"), 1L, 0);
        assertThat(bucketKeyEncoder).isNotSameAs(primaryKeyEncoder);
        assertThat(bucketKeyEncoder.encodeKey(row))
                .isEqualTo(
                        new IcebergKeyEncoder(ROW_TYPE, Collections.singletonList("a"))
                                .encodeKey(row));
    }

    private static TableConfig icebergTableConfig(int kvFormatVersion) {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.ICEBERG);
        configuration.set(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        return new TableConfig(configuration);
    }
}
