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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

/**
 * The scanner that reads records form a table in a batch fashion. Compared to {@link LogScanner},
 * this scanner is designed to read bounded data and will stop when reading end of a bucket, but
 * {@link LogScanner} is designed to read unbounded data and continuously read data from buckets.
 *
 * @since 0.6
 */
@PublicEvolving
public interface BatchScanner extends Closeable {

    /**
     * Configure the scanner to read a snapshot over the whole table (non-partitioned tables).
     * For partitioned tables, use {@link #snapshotAllPartition(String)} to select a partition.
     *
     * <p>By default, this is a no-op and returns {@code this}. Implementations may override to
     * return a new configured scanner instance.
     */
    default BatchScanner snapshotAll() {
        return this;
    }

    /**
     * Configure the scanner to read a snapshot from the specified partition. Only applicable to
     * partitioned tables.
     *
     * @param partitionName the partition to scan
     * @return a scanner configured for a one-shot snapshot over the given partition
     * @throws UnsupportedOperationException if the table is not partitioned
     */
    default BatchScanner snapshotAllPartition(String partitionName) {
        throw new UnsupportedOperationException(
                "Partition filter is only supported for partitioned tables");
    }

    /**
     * Poll one batch records. The method should return null when reaching the end of the input.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE}
     *     milliseconds)
     */
    @Nullable
    CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException;

    /** Closes the scanner and should release all resources. */
    @Override
    void close() throws IOException;
}
