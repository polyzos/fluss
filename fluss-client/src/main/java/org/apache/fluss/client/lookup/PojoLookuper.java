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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.converter.PojoToRowConverter;
import org.apache.fluss.client.converter.RowToPojoConverter;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Utility that keeps the original Lookuper API intact while offering POJO-based lookup ergonomics.
 *
 * <p>This class wraps a {@link Lookuper} and performs POJO-to-row conversion for keys and
 * row-to-POJO conversion for results using the existing converters. It does not alter the public
 * {@link Lookup} API, preserving backward compatibility.
 */
@PublicEvolving
public final class PojoLookuper<K, R> {

    private final Lookuper delegate;
    private final PojoToRowConverter<K> keyConverter;
    private final RowToPojoConverter<R> resultConverter;

    private PojoLookuper(Lookuper delegate,
                         PojoToRowConverter<K> keyConverter,
                         RowToPojoConverter<R> resultConverter) {
        this.delegate = delegate;
        this.keyConverter = keyConverter;
        this.resultConverter = resultConverter;
    }

    /**
     * Creates a POJO lookuper for primary-key lookup using the table's full schema as result type
     * and the primary key projection for the key.
     */
    public static <K, R> PojoLookuper<K, R> forPrimaryKey(Table table,
                                                          Class<K> keyClass,
                                                          Class<R> resultClass) {
        Objects.requireNonNull(table, "table");
        TableInfo tableInfo = table.getTableInfo();
        RowType tableSchema = tableInfo.getRowType();
        RowType keyProjection = tableSchema.project(tableInfo.getPrimaryKeys());
        PojoToRowConverter<K> keyConv = PojoToRowConverter.of(keyClass, tableSchema, keyProjection);
        RowToPojoConverter<R> resConv = RowToPojoConverter.of(resultClass, tableSchema, tableSchema);
        Lookuper base = table.newLookup().createLookuper();
        return new PojoLookuper<>(base, keyConv, resConv);
    }

    /**
     * Creates a POJO lookuper for prefix-key lookup using the provided lookup columns.
     */
    public static <K, R> PojoLookuper<K, R> forPrefixKey(Table table,
                                                         List<String> lookupColumns,
                                                         Class<K> keyClass,
                                                         Class<R> resultClass) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(lookupColumns, "lookupColumns");
        TableInfo tableInfo = table.getTableInfo();
        RowType tableSchema = tableInfo.getRowType();
        RowType keyProjection = tableSchema.project(lookupColumns);
        PojoToRowConverter<K> keyConv = PojoToRowConverter.of(keyClass, tableSchema, keyProjection);
        RowToPojoConverter<R> resConv = RowToPojoConverter.of(resultClass, tableSchema, tableSchema);
        Lookuper base = table.newLookup().lookupBy(lookupColumns).createLookuper();
        return new PojoLookuper<>(base, keyConv, resConv);
    }

    /**
     * Performs a lookup using a POJO key and returns POJO results.
     */
    public CompletableFuture<PojoLookupResult<R>> lookup(K lookupKey) {
        InternalRow keyRow = keyConverter.toRow(lookupKey);
        return delegate
                .lookup(keyRow)
                .thenApply(result -> {
                    List<InternalRow> rows = result.getRowList();
                    if (rows == null || rows.isEmpty()) {
                        return new PojoLookupResult<R>((R) null);
                    }
                    List<R> out = new ArrayList<>(rows.size());
                    for (InternalRow row : rows) {
                        out.add(resultConverter.fromRow(row));
                    }
                    return new PojoLookupResult<>(out);
                });
    }
}
