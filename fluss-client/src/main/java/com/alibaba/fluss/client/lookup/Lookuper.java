/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.lookup;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.row.RowSerializer;
import com.alibaba.fluss.row.InternalRow;

import java.util.concurrent.CompletableFuture;

/**
 * The lookup-er is used to lookup row of a primary key table by primary key or prefix key.
 *
 * @param <T> The type of data returned. Can be {@link InternalRow} or a POJO type.
 * @since 0.6
 */
@PublicEvolving
public interface Lookuper<T> {

    /**
     * Lookups certain row from the given lookup key.
     *
     * <p>The lookup key must be a primary key if the lookuper is a Primary Key Lookuper (created by
     * {@code table.newLookuper().create()}), or be the prefix key if the lookuper is a Prefix Key
     * Lookuper (created by {@code table.newLookuper().withLookupColumns(prefixKeys).create()}).
     *
     * @param lookupKey the lookup key.
     * @return the result of lookup.
     */
    CompletableFuture<LookupResult<T>> lookup(InternalRow lookupKey);

    /**
     * Creates a new Lookuper that returns POJOs of type P by converting InternalRows using the
     * provided converter.
     *
     * @param rowSerializer The rowSerializer to use for converting InternalRows to POJOs
     * @param <P> The POJO type
     * @return A new Lookuper that returns POJOs
     */
    <P> Lookuper<P> withRowSerializer(RowSerializer<P> rowSerializer);
}
