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
 * The writer to write data to the primary key table.
 *
 * @param <T> The type of data to write. Can be {@link InternalRow} or a POJO type.
 * @since 0.2
 */
@PublicEvolving
public interface UpsertWriter<T> extends TableWriter {

    /**
     * Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
     *
     * @param row the row to upsert.
     * @return A {@link CompletableFuture} that always returns upsert result when complete normally.
     */
    CompletableFuture<UpsertResult> upsert(T row);

    /**
     * Delete certain row by the input row in Fluss table, the input row must contain the primary
     * key.
     *
     * @param row the row to delete.
     * @return A {@link CompletableFuture} that always delete result when complete normally.
     */
    CompletableFuture<DeleteResult> delete(T row);

    /**
     * Creates a new UpsertWriter that accepts POJOs of type P and converts them to InternalRow
     * using the provided converter.
     *
     * @param rowSerializer The rowSerializer to use for converting POJOs to InternalRow
     * @param <P> The POJO type
     * @return A new UpsertWriter that accepts POJOs
     */
    <P> UpsertWriter<P> withRowSerializer(RowSerializer<P> rowSerializer);
}
