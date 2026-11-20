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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussPaths}. */
class FlussPathsTest {

    @Test
    void testFilenamePrefixFromOffset() {
        assertThat(FlussPaths.filenamePrefixFromOffset(0L)).isEqualTo("00000000000000000000");
        assertThat(FlussPaths.filenamePrefixFromOffset(1234567L)).isEqualTo("00000000000001234567");
        assertThat(FlussPaths.filenamePrefixFromOffset(Long.MAX_VALUE))
                .isEqualTo("09223372036854775807");
    }

    @Test
    void testFilenamePrefixFromOffsetIgnoresDefaultLocale() {
        Locale previous = Locale.getDefault();
        // a locale that formats numbers with Thai digits and grouping separators by default
        Locale.setDefault(Locale.forLanguageTag("th-TH-u-nu-thai"));
        try {
            assertThat(FlussPaths.filenamePrefixFromOffset(1234567L))
                    .isEqualTo("00000000000001234567");
        } finally {
            Locale.setDefault(previous);
        }
    }
}
