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

package org.apache.fluss.client.table.scanner;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteFileDownloader}. */
class RemoteFileDownloaderTest {

    @Test
    void testDownloadFileAsyncCopiesExactBytes() throws Exception {
        // create a source file with random data
        Path sourceDir = Files.createTempDirectory("rfd-src");
        Path targetDir = Files.createTempDirectory("rfd-dst");
        Path sourceFile = sourceDir.resolve("blob.bin");
        byte[] data = new byte[8192];
        new Random(1234).nextBytes(data);
        Files.write(sourceFile, data);

        FsPath remoteDir = new FsPath(sourceDir.toUri().toString());
        FsPathAndFileName remote = new FsPathAndFileName(remoteDir, sourceFile.getFileName().toString());

        RemoteFileDownloader downloader = new RemoteFileDownloader(2);
        try {
            long bytes = downloader.downloadFileAsync(remote, targetDir).get();
            assertThat(bytes).isEqualTo(data.length);
            Path copied = targetDir.resolve(sourceFile.getFileName());
            assertThat(Files.exists(copied)).isTrue();
            byte[] copiedData = Files.readAllBytes(copied);
            assertThat(copiedData).isEqualTo(data);
        } finally {
            downloader.close();
            // cleanup
            deleteQuietly(targetDir);
            deleteQuietly(sourceDir);
        }
    }

    private static void deleteQuietly(Path path) throws IOException {
        if (path == null) return;
        if (Files.notExists(path)) return;
        Files.walk(path)
                .sorted((a,b) -> b.getNameCount()-a.getNameCount())
                .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignore) {} });
    }
}
