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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.row.InternalRow;

import java.util.concurrent.CompletableFuture;

/**
 * The writer to write data to the log table.
 *
 * @param <T> The type of data to write. Can be {@link InternalRow} or a POJO type.
 * @since 0.2
 */
@PublicEvolving
public interface AppendWriter<T> extends TableWriter {

    /**
     * Append row into a Log Table.
     *
     * @param row the row to append.
     * @return A {@link CompletableFuture} that always returns append result when complete normally.
     */
    CompletableFuture<AppendResult> append(T row);

    /**
     * Creates a new AppendWriter that accepts POJOs of type P and converts them to InternalRow
     * using the provided converter.
     *
     * @param rowSerializer The rowSerializer to use for converting POJOs to InternalRow
     * @param <P> The POJO type
     * @return A new AppendWriter that accepts POJOs
     */
    <P> AppendWriter<P> withRowSerializer(RowSerializer<P> rowSerializer);
}
