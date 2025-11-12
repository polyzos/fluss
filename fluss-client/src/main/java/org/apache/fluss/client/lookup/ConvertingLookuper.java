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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.converter.PojoToRowConverter;
import org.apache.fluss.client.converter.RowToPojoConverter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Adapter that allows using POJOs for key and result in lookups. */
class ConvertingLookuper<K, R> implements TypedLookuper<K, R> {

    private final Lookuper delegate;
    private final PojoToRowConverter<K> keyConverter;
    private final RowToPojoConverter<R> resultConverter;

    ConvertingLookuper(Lookuper delegate,
                       Class<K> keyClass,
                       Class<R> resultClass,
                       TableInfo tableInfo,
                       @Nullable List<String> lookupColumnNames) {
        this.delegate = delegate;
        RowType tableSchema = tableInfo.getRowType();
        RowType keyProjection;
        if (lookupColumnNames == null) {
            keyProjection = tableSchema.project(tableInfo.getPrimaryKeys());
        } else {
            keyProjection = tableSchema.project(lookupColumnNames);
        }
        this.keyConverter = PojoToRowConverter.of(keyClass, tableSchema, keyProjection);
        this.resultConverter = RowToPojoConverter.of(resultClass, tableSchema, tableSchema);
    }

    @Override
    public CompletableFuture<TypedLookupResult<R>> lookup(K lookupKey) {
        InternalRow keyRow = keyConverter.toRow(lookupKey);
        return delegate
                .lookup(keyRow)
                .thenApply(result -> {
                    List<InternalRow> rows = result.getRowList();
                    if (rows == null || rows.isEmpty()) {
                        return new TypedLookupResult<R>((R) null);
                    }
                    List<R> out = new ArrayList<>(rows.size());
                    for (InternalRow row : rows) {
                        out.add(resultConverter.fromRow(row));
                    }
                    return new TypedLookupResult<>(out);
                });
    }
}
