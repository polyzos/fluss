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

package org.apache.fluss.bucketing;

import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Fluss bucket of a row in an Iceberg table with a multi-column bucket key is
 * exactly Iceberg's {@code bucket[N]} of the first bucket key column, which is the partition value
 * Iceberg engines use for pruning.
 */
class IcebergCompositeBucketKeyBucketingTest {

    private static final int ROWS = 1000;
    private static final int[] BUCKET_NUMS = {1, 3, 7, 16};

    @Test
    void testIntFirstColumn() {
        verifyParity(DataTypes.INT(), Types.IntegerType.get(), Random::nextInt, value -> value);
    }

    @Test
    void testBigIntFirstColumn() {
        verifyParity(DataTypes.BIGINT(), Types.LongType.get(), Random::nextLong, value -> value);
    }

    @Test
    void testStringFirstColumn() {
        verifyParity(
                DataTypes.STRING(),
                Types.StringType.get(),
                random -> BinaryString.fromString("key-" + random.nextLong()),
                BinaryString::toString);
    }

    @Test
    void testDecimalFirstColumn() {
        verifyParity(
                DataTypes.DECIMAL(18, 4),
                Types.DecimalType.of(18, 4),
                random -> Decimal.fromBigDecimal(BigDecimal.valueOf(random.nextInt(), 4), 18, 4),
                Decimal::toBigDecimal);
    }

    @Test
    void testDateFirstColumn() {
        verifyParity(
                DataTypes.DATE(),
                Types.DateType.get(),
                random -> random.nextInt(100_000),
                value -> value);
    }

    @Test
    void testTimestampFirstColumn() {
        verifyParity(
                DataTypes.TIMESTAMP(6),
                Types.TimestampType.withoutZone(),
                random -> TimestampNtz.fromMillis(Math.abs(random.nextLong() % 4_000_000_000_000L)),
                value -> value.getMillisecond() * 1000L + value.getNanoOfMillisecond() / 1000);
    }

    @Test
    void testBytesFirstColumn() {
        verifyParity(
                DataTypes.BYTES(),
                Types.BinaryType.get(),
                random -> {
                    byte[] bytes = new byte[1 + random.nextInt(16)];
                    random.nextBytes(bytes);
                    return bytes;
                },
                ByteBuffer::wrap);
    }

    private static <T> void verifyParity(
            DataType flussType,
            Type icebergType,
            Function<Random, T> valueGenerator,
            Function<T, Object> toIcebergValue) {
        RowType rowType =
                RowType.of(
                        new DataType[] {flussType, DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"k1", "k2", "k3"});
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        rowType, Arrays.asList("k1", "k2", "k3"), DataLakeFormat.ICEBERG);
        BucketingFunction bucketingFunction = BucketingFunction.of(DataLakeFormat.ICEBERG);

        Random random = new Random(42);
        for (int i = 0; i < ROWS; i++) {
            T firstValue = valueGenerator.apply(random);
            GenericRow row =
                    GenericRow.of(
                            firstValue,
                            BinaryString.fromString("k2-" + random.nextInt()),
                            random.nextLong());
            byte[] bucketKey = bucketKeyEncoder.encodeKey(row);
            for (int bucketNum : BUCKET_NUMS) {
                Transform<Object, Integer> transform = Transforms.bucket(bucketNum);
                int icebergBucket =
                        transform.bind(icebergType).apply(toIcebergValue.apply(firstValue));
                assertThat(bucketingFunction.bucketing(bucketKey, bucketNum))
                        .as("bucket of %s with %s buckets", firstValue, bucketNum)
                        .isEqualTo(icebergBucket);
            }
        }
    }
}
