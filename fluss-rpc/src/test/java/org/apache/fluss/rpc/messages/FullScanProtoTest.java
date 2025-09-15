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

import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.Errors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for FULL_SCAN request/response wire format. */
class FullScanProtoTest {

    @Test
    void testApiKeyPresence() {
        assertThat(ApiKeys.FULL_SCAN).isNotNull();
        assertThat(ApiKeys.FULL_SCAN.visibility).isEqualTo(ApiKeys.ApiVisibility.PUBLIC);
    }

    @Test
    void testRequestRoundTrip() {
        FullScanRequest req = new FullScanRequest().setTableId(123L).setPartitionId(55L);
        byte[] bytes = req.toByteArray();
        FullScanRequest parsed = new FullScanRequest();
        parsed.parseFrom(bytes);
        assertThat(parsed.hasTableId()).isTrue();
        assertThat(parsed.getTableId()).isEqualTo(123L);
        assertThat(parsed.hasPartitionId()).isTrue();
        assertThat(parsed.getPartitionId()).isEqualTo(55L);
    }

    @Test
    void testResponseRoundTripWithRecordsAndMetrics() {
        FullScanResponse resp = new FullScanResponse();
        byte[] payload = new byte[] {1, 2, 3, 4};
        resp.setRecords(payload);
        resp.setEstimatedKeyCount(1000L);
        resp.setElapsedMs(42L);
        byte[] bytes = resp.toByteArray();
        FullScanResponse parsed = new FullScanResponse();
        parsed.parseFrom(bytes);
        assertThat(parsed.hasRecords()).isTrue();
        assertThat(parsed.getRecordsSize()).isEqualTo(payload.length);
        assertThat(parsed.hasEstimatedKeyCount()).isTrue();
        assertThat(parsed.getEstimatedKeyCount()).isEqualTo(1000L);
        assertThat(parsed.hasElapsedMs()).isTrue();
        assertThat(parsed.getElapsedMs()).isEqualTo(42L);
    }

    @Test
    void testErrorMapping() {
        FullScanResponse err = new FullScanResponse();
        err.setErrorCode(Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION.code());
        err.setErrorMessage("not a pk table");
        byte[] bytes = err.toByteArray();
        FullScanResponse parsed = new FullScanResponse();
        parsed.parseFrom(bytes);
        assertThat(parsed.hasErrorCode()).isTrue();
        assertThat(parsed.getErrorCode()).isEqualTo(Errors.NON_PRIMARY_KEY_TABLE_EXCEPTION.code());
        assertThat(parsed.hasErrorMessage()).isTrue();
        assertThat(parsed.getErrorMessage()).contains("not a pk table");
    }
}
