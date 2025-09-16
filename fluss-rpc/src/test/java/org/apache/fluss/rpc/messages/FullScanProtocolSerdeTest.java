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

package org.apache.fluss.rpc.messages;

import org.apache.fluss.rpc.protocol.Errors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Serde tests for FULL_SCAN request/response. */
class FullScanProtocolSerdeTest {

    @Test
    void testRequestRoundTrip() {
        FullScanRequest req = new FullScanRequest().setTableId(123L).setPartitionId(456L);
        byte[] bytes = req.toByteArray();
        FullScanRequest parsed = new FullScanRequest();
        parsed.parseFrom(bytes);
        assertThat(parsed.hasTableId()).isTrue();
        assertThat(parsed.getTableId()).isEqualTo(123L);
        assertThat(parsed.hasPartitionId()).isTrue();
        assertThat(parsed.getPartitionId()).isEqualTo(456L);

        FullScanRequest copy = new FullScanRequest().copyFrom(parsed);
        assertThat(copy.getTableId()).isEqualTo(123L);
        assertThat(copy.getPartitionId()).isEqualTo(456L);
    }

    @Test
    void testResponseRoundTrip() {
        byte[] records = new byte[] {1, 2, 3, 4};
        FullScanResponse resp =
                new FullScanResponse()
                        .setErrorCode(Errors.NONE.code())
                        .setIsLogTable(false)
                        .setRecords(records);

        byte[] bytes = resp.toByteArray();
        FullScanResponse parsed = new FullScanResponse();
        parsed.parseFrom(bytes);

        assertThat(parsed.hasErrorCode()).isTrue();
        assertThat(parsed.getErrorCode()).isEqualTo(Errors.NONE.code());
        assertThat(parsed.hasIsLogTable()).isTrue();
        assertThat(parsed.isIsLogTable()).isFalse();
        assertThat(parsed.hasRecords()).isTrue();
        assertThat(parsed.getRecordsSize()).isEqualTo(records.length);

        FullScanResponse copy = new FullScanResponse().copyFrom(parsed);
        assertThat(copy.getRecords()).containsExactly(records);
    }
}
