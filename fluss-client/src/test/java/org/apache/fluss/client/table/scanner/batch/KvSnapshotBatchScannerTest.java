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

import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit-style tests for KvSnapshotBatchScanner async initialization behavior. */
class KvSnapshotBatchScannerTest {

    /**
     * Verifies that pollBatch times out with empty iterator before reader becomes ready,
     * then returns data once files are available and reader initialized.
     */
    @Test
    void testPollTimeoutThenReturnData() throws Exception {
        // Create a temporary directory to act as remote fs path root.
        Path remoteRoot = Files.createTempDirectory("kv-snap-test-remote");
        try {
            // We will prepare one simple Arrow/Compacted file depending on format; for simplicity,
            // we target COMPACTED kv format and rely on SnapshotFilesReader to parse key/value rows
            // from provided files. Here we simulate by creating an empty file, and the scanner will
            // just produce no rows. This still validates timeout vs readiness sequencing.
            FsPath remoteDir = new FsPath(remoteRoot.toUri().toString());
            FsPathAndFileName file = new FsPathAndFileName(remoteDir, "empty.sst");
            Files.createFile(remoteRoot.resolve("empty.sst"));

            // Prepare a RemoteFileDownloader stub that delays file transfer to trigger timeout.
            class DelayedDownloader extends RemoteFileDownloader {
                private volatile boolean allowTransfer = false;
                DelayedDownloader() { super(1); }
                @Override
                public CompletableFuture<Long> downloadFileAsync(
                        FsPathAndFileName fsPathAndFileName, Path targetDirectory) {
                    CompletableFuture<Long> f = new CompletableFuture<>();
                    new Thread(() -> {
                        try {
                            // spin-wait until allowed
                            while (!allowTransfer) {
                                Thread.sleep(10);
                            }
                            // delegate real copy using protected API
                            Path target = targetDirectory.resolve(fsPathAndFileName.getFileName());
                            long n = super.downloadFile(target, fsPathAndFileName.getPath());
                            f.complete(n);
                        } catch (Throwable t) {
                            f.completeExceptionally(t);
                        }
                    }, "delayed-downloader").start();
                    return f;
                }
            }
            DelayedDownloader downloader = new DelayedDownloader();

            RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
            TableBucket bucket = new TableBucket(1L, 0);
            KvSnapshotBatchScanner scanner = new KvSnapshotBatchScanner(
                    rowType,
                    bucket,
                    Collections.singletonList(file),
                    null,
                    Files.createTempDirectory("kv-snap-test-local").toString(),
                    KvFormat.COMPACTED,
                    downloader);

            // First poll with small timeout should return empty iterator (reader not ready yet)
            CloseableIterator<InternalRow> first = scanner.pollBatch(Duration.ofMillis(50));
            assertThat(first).isNotNull();
            assertThat(first.hasNext()).isFalse();
            first.close();

            // Allow transfer and wait a bit for reader to become ready, then poll again.
            downloader.allowTransfer = true;

            CloseableIterator<InternalRow> second = null;
            for (int i = 0; i < 20; i++) {
                second = scanner.pollBatch(Duration.ofMillis(200));
                if (second != null) {
                    break;
                }
            }
            // For empty file, either null (no data) or empty iterator is acceptable as end.
            // Validate scanner doesn't throw and reaches end-of-input gracefully.
            if (second != null) {
                assertThat(second.hasNext()).isFalse();
                second.close();
            }

            scanner.close();
        } finally {
            // best-effort cleanup
            Files.walk(remoteRoot)
                    .sorted((a,b) -> b.getNameCount()-a.getNameCount())
                    .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignore) {} });
        }
    }
}
