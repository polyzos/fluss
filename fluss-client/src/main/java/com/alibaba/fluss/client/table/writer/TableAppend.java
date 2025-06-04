/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.writer;

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;

/** API for configuring and creating {@link AppendWriter}. */
public class TableAppend implements Append {

    private final TablePath tablePath;
    private final TableInfo tableInfo;
    private final MetadataUpdater metadataUpdater;
    private final WriterClient writerClient;

    public TableAppend(
            TablePath tablePath,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            WriterClient writerClient) {
        this.tablePath = tablePath;
        this.tableInfo = tableInfo;
        this.metadataUpdater = metadataUpdater;
        this.writerClient = writerClient;
    }

    @Override
    public AppendWriter<InternalRow> createWriter() {
        return new AppendWriterImpl<>(tablePath, tableInfo, metadataUpdater, writerClient);
    }
}
