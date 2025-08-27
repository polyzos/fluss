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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.metadata.TableInfo;

/**
 * A clearer name for scanning the full contents of a Primary-Key table across all buckets.
 *
 * <p>This class currently extends the existing WholePkTableBatchScanner for backward compatibility
 * and to keep changes minimal. In future, the old class can be removed and logic consolidated here.
 */
@Internal
public class FullPkTableScanner extends WholePkTableBatchScanner {

    public FullPkTableScanner(
            FlussConnection conn,
            TableInfo tableInfo,
            Admin admin,
            RemoteFileDownloader remoteFileDownloader) {
        super(conn, tableInfo, admin, remoteFileDownloader);
    }
}
