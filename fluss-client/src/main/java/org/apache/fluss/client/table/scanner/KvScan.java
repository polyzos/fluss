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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

/**
 * Used to configure and execute a full scan over the KV (RocksDB) state of a primary key table.
 *
 * <p>The scan reads all live rows from every bucket, handling partitioning and bucket enumeration
 * automatically. Each call to {@link #execute()} opens a point-in-time RocksDB snapshot on each
 * tablet server and streams the rows back to the client.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try (CloseableIterator<InternalRow> rows = table.newKvScan().execute()) {
 *     while (rows.hasNext()) {
 *         InternalRow row = rows.next();
 *         // process row
 *     }
 * }
 * }</pre>
 *
 * @since 0.9
 */
@PublicEvolving
public interface KvScan {

    /**
     * Executes the KV scan to read all current data in the table. Partitions and buckets are
     * enumerated automatically; the caller only needs to iterate the returned rows.
     *
     * @return a closeable iterator of the rows in the table; must be closed after use
     * @throws org.apache.fluss.exception.FlussRuntimeException if partition enumeration fails or
     *     an error occurs while reading rows from the server
     */
    CloseableIterator<InternalRow> execute();
}
