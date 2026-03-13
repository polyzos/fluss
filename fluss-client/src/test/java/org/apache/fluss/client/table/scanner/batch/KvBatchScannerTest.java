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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.client.metadata.TestingMetadataUpdater;
import org.apache.fluss.exception.ScannerExpiredException;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link KvBatchScanner} error handling and close behaviour. */
class KvBatchScannerTest {

    // Bucket 0 is assigned to NODE1 (id=1) by TestingMetadataUpdater.
    private static final TableBucket BUCKET = new TableBucket(DATA1_TABLE_ID, 0);
    private static final SchemaInfo SCHEMA_INFO =
            new SchemaInfo(DATA1_SCHEMA, DEFAULT_SCHEMA_ID);

    /**
     * When the server returns a response with a non-zero {@code error_code}, {@link
     * KvBatchScanner#pollBatch} must throw an {@link IOException} whose cause is the corresponding
     * Fluss exception — not silently return {@code null} as if the scan completed.
     */
    @Test
    void testPollBatch_throwsIoExceptionOnErrorCode() {
        ScanKvResponse errorResponse =
                new ScanKvResponse()
                        .setErrorCode(Errors.SCANNER_EXPIRED.code())
                        .setErrorMessage("Scanner session has expired due to inactivity.");

        TestTabletServerGateway errorGateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
                        return CompletableFuture.completedFuture(errorResponse);
                    }
                };

        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(1, errorGateway)
                        .build();

        try (KvBatchScanner scanner =
                new KvBatchScanner(
                        DATA1_TABLE_INFO,
                        BUCKET,
                        new TestingSchemaGetter(SCHEMA_INFO),
                        metadataUpdater)) {
            assertThatThrownBy(() -> scanner.pollBatch(Duration.ofSeconds(5)))
                    .isInstanceOf(IOException.class)
                    .hasCauseInstanceOf(ScannerExpiredException.class)
                    .hasMessageContaining("Scanner session has expired");
        } catch (IOException ignored) {
            // close() after the exception is benign
        }
    }

    /**
     * When the server returns an error code without an error message, {@link
     * KvBatchScanner#pollBatch} must still throw an {@link IOException} with a non-null cause.
     */
    @Test
    void testPollBatch_throwsIoExceptionOnErrorCodeWithoutMessage() {
        ScanKvResponse errorResponse =
                new ScanKvResponse().setErrorCode(Errors.UNKNOWN_SCANNER_ID.code());

        TestTabletServerGateway errorGateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
                        return CompletableFuture.completedFuture(errorResponse);
                    }
                };

        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(1, errorGateway)
                        .build();

        try (KvBatchScanner scanner =
                new KvBatchScanner(
                        DATA1_TABLE_INFO,
                        BUCKET,
                        new TestingSchemaGetter(SCHEMA_INFO),
                        metadataUpdater)) {
            assertThatThrownBy(() -> scanner.pollBatch(Duration.ofSeconds(5)))
                    .isInstanceOf(IOException.class)
                    .hasCause(Errors.UNKNOWN_SCANNER_ID.exception());
        } catch (IOException ignored) {
            // close() after the exception is benign
        }
    }

    /**
     * When the {@code close_scanner} RPC fails (e.g., because the leader has changed), {@link
     * KvBatchScanner#close} must swallow the exception and not propagate it to the caller. The
     * server-side TTL reaper will eventually clean up the orphaned session.
     */
    @Test
    void testClose_doesNotPropagateRpcFailure() throws Exception {
        byte[] scannerId = new byte[] {1, 2, 3};
        AtomicInteger callCount = new AtomicInteger(0);

        TestTabletServerGateway gateway =
                new TestTabletServerGateway(false, Collections.emptySet()) {
                    @Override
                    public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
                        int call = callCount.incrementAndGet();
                        if (call == 1) {
                            // Initial openScanner response: echo back the scannerId and
                            // indicate there are more results so the scanner stays open.
                            return CompletableFuture.completedFuture(
                                    new ScanKvResponse()
                                            .setScannerId(scannerId)
                                            .setHasMoreResults(true));
                        }
                        if (request.hasCloseScanner() && request.isCloseScanner()) {
                            // Simulate a failed close_scanner RPC.
                            CompletableFuture<ScanKvResponse> failed = new CompletableFuture<>();
                            failed.completeExceptionally(
                                    new RuntimeException("Leader not available"));
                            return failed;
                        }
                        // Continuation request: never completes (will be cancelled by close).
                        return new CompletableFuture<>();
                    }
                };

        TestingMetadataUpdater metadataUpdater =
                TestingMetadataUpdater.builder(
                                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO))
                        .withTabletServerGateway(1, gateway)
                        .build();

        KvBatchScanner scanner =
                new KvBatchScanner(
                        DATA1_TABLE_INFO,
                        BUCKET,
                        new TestingSchemaGetter(SCHEMA_INFO),
                        metadataUpdater);

        // First pollBatch: fires openScanner, gets {scannerId, hasMoreResults=true}, fires a
        // prefetch continuation (call 2), then returns an empty iterator (no records).
        scanner.pollBatch(Duration.ofSeconds(5));
        assertThat(callCount.get()).isEqualTo(2);

        // close(): cancels the pending prefetch and sends the close_scanner RPC (call 3) which
        // fails. Must NOT throw.
        assertThatCode(scanner::close).doesNotThrowAnyException();
    }
}
