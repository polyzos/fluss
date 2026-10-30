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

import org.apache.fluss.exception.InvalidTableException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IcebergBucketing}. */
class IcebergBucketingTest {

    private static final List<String> PRIMARY_KEY = Arrays.asList("a", "b", "c");

    @Test
    void testHashKeyIsFirstBucketKey() {
        assertThat(IcebergBucketing.hashKey(Collections.singletonList("a"))).isEqualTo("a");
        assertThat(IcebergBucketing.hashKey(Arrays.asList("b", "a"))).isEqualTo("b");
        assertThatThrownBy(() -> IcebergBucketing.hashKey(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSingleColumnBucketKeyIsAlwaysSupported() {
        List<String> single = Collections.singletonList("a");
        // log table, legacy v1 primary key table, v2 primary key table
        assertThatCode(() -> IcebergBucketing.validateBucketKeys(single, emptyList(), null))
                .doesNotThrowAnyException();
        assertThatCode(() -> IcebergBucketing.validateBucketKeys(single, PRIMARY_KEY, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> IcebergBucketing.validateBucketKeys(single, PRIMARY_KEY, 2))
                .doesNotThrowAnyException();
        // default single-column bucket key
        assertThatCode(() -> IcebergBucketing.validateBucketKeys(single, single, 2))
                .doesNotThrowAnyException();
        // no bucket key
        assertThatCode(() -> IcebergBucketing.validateBucketKeys(emptyList(), emptyList(), null))
                .doesNotThrowAnyException();
    }

    @Test
    void testStrictPrefixOfPrimaryKeyIsSupported() {
        assertThatCode(
                        () ->
                                IcebergBucketing.validateBucketKeys(
                                        Arrays.asList("a", "b"), PRIMARY_KEY, 2))
                .doesNotThrowAnyException();
    }

    @Test
    void testMultiColumnBucketKeyOnLogTableIsRejected() {
        assertThatThrownBy(
                        () ->
                                IcebergBucketing.validateBucketKeys(
                                        Arrays.asList("a", "b"), emptyList(), null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("the table is a log table")
                .hasMessageContaining("distributed by the first bucket key column only");
    }

    @Test
    void testMultiColumnBucketKeyOnLegacyKvFormatIsRejected() {
        assertThatThrownBy(
                        () ->
                                IcebergBucketing.validateBucketKeys(
                                        Arrays.asList("a", "b"), PRIMARY_KEY, null))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("must use kv format version 2, but uses version 1");
        assertThatThrownBy(
                        () ->
                                IcebergBucketing.validateBucketKeys(
                                        Arrays.asList("a", "b"), PRIMARY_KEY, 1))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("must use kv format version 2, but uses version 1");
    }

    @Test
    void testMultiColumnBucketKeyEqualToPrimaryKeyIsRejected() {
        // also covers the default bucket key of a primary key table with a composite key
        assertThatThrownBy(() -> IcebergBucketing.validateBucketKeys(PRIMARY_KEY, PRIMARY_KEY, 2))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("must be a strict prefix of the primary key [a, b, c]");
    }

    @Test
    void testMultiColumnBucketKeyNotAnOrderedPrefixIsRejected() {
        for (List<String> bucketKeys :
                Arrays.asList(
                        Arrays.asList("b", "a"),
                        Arrays.asList("a", "c"),
                        Arrays.asList("b", "c"))) {
            assertThatThrownBy(
                            () -> IcebergBucketing.validateBucketKeys(bucketKeys, PRIMARY_KEY, 2))
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("must be a strict prefix of the primary key");
        }
    }

    private static List<String> emptyList() {
        return Collections.emptyList();
    }
}
