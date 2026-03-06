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

import org.apache.fluss.client.FlussConnection;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Implementation of {@link KvScan}. */
public class TableKvScan implements KvScan {

    /** Timeout used when polling each batch from the server-side scanner. */
    private static final Duration BATCH_POLL_TIMEOUT = Duration.ofSeconds(30);

    private final FlussConnection conn;
    private final TableInfo tableInfo;
    private final SchemaGetter schemaGetter;

    public TableKvScan(FlussConnection conn, TableInfo tableInfo, SchemaGetter schemaGetter) {
        this.conn = conn;
        this.tableInfo = tableInfo;
        this.schemaGetter = schemaGetter;
    }

    @Override
    public CloseableIterator<InternalRow> execute() {
        List<TableBucket> buckets = new ArrayList<>();
        try {
            if (tableInfo.isPartitioned()) {
                List<PartitionInfo> partitions =
                        conn.getAdmin().listPartitionInfos(tableInfo.getTablePath()).get();
                for (PartitionInfo partition : partitions) {
                    for (int i = 0; i < tableInfo.getNumBuckets(); i++) {
                        buckets.add(
                                new TableBucket(
                                        tableInfo.getTableId(), partition.getPartitionId(), i));
                    }
                }
            } else {
                for (int i = 0; i < tableInfo.getNumBuckets(); i++) {
                    buckets.add(new TableBucket(tableInfo.getTableId(), i));
                }
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    "Failed to list partitions for table " + tableInfo.getTablePath(), e);
        }

        Scan scan = new TableScan(conn, tableInfo, schemaGetter);
        return new MultiBucketIterator(buckets, scan);
    }

    private static CloseableIterator<InternalRow> scanBucket(Scan scan, TableBucket tableBucket) {
        BatchScanner batchScanner = scan.createBatchScanner(tableBucket);
        return new BatchScannerIterator(batchScanner);
    }

    private static class MultiBucketIterator implements CloseableIterator<InternalRow> {
        private final Iterator<TableBucket> bucketIterator;
        private final Scan scan;
        private CloseableIterator<InternalRow> currentScannerIterator;
        private boolean isClosed = false;

        private MultiBucketIterator(List<TableBucket> buckets, Scan scan) {
            this.bucketIterator = buckets.iterator();
            this.scan = scan;
        }

        @Override
        public boolean hasNext() {
            if (isClosed) {
                return false;
            }
            while (currentScannerIterator == null || !currentScannerIterator.hasNext()) {
                if (currentScannerIterator != null) {
                    currentScannerIterator.close();
                    currentScannerIterator = null;
                }
                if (bucketIterator.hasNext()) {
                    currentScannerIterator = scanBucket(scan, bucketIterator.next());
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentScannerIterator.next();
        }

        @Override
        public void close() {
            if (!isClosed) {
                if (currentScannerIterator != null) {
                    currentScannerIterator.close();
                }
                isClosed = true;
            }
        }
    }

    private static class BatchScannerIterator implements CloseableIterator<InternalRow> {
        private final BatchScanner scanner;
        private Iterator<InternalRow> currentBatch;
        private boolean isClosed = false;

        private BatchScannerIterator(BatchScanner scanner) {
            this.scanner = scanner;
        }

        @Override
        public boolean hasNext() {
            ensureBatch();
            return currentBatch != null && currentBatch.hasNext();
        }

        @Override
        public InternalRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentBatch.next();
        }

        private void ensureBatch() {
            try {
                while ((currentBatch == null || !currentBatch.hasNext()) && !isClosed) {
                    CloseableIterator<InternalRow> it = scanner.pollBatch(BATCH_POLL_TIMEOUT);
                    if (it == null) {
                        isClosed = true;
                        break;
                    }
                    if (it.hasNext()) {
                        currentBatch = it;
                    } else {
                        it.close();
                    }
                }
            } catch (IOException e) {
                throw new FlussRuntimeException("Error polling batch from scanner", e);
            }
        }

        @Override
        public void close() {
            if (!isClosed) {
                try {
                    scanner.close();
                } catch (IOException e) {
                    throw new FlussRuntimeException("Error closing scanner", e);
                }
                isClosed = true;
            }
        }
    }
}
