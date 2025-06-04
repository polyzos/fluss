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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The result of {@link Lookuper#lookup(InternalRow)}.
 *
 * @param <T> The type of data returned. Can be {@link InternalRow} or a POJO type.
 * @since 0.1
 */
@PublicEvolving
public final class LookupResult<T> {
    private final List<T> rowList;
    private final @Nullable RowSerializer<T> rowSerializer;

    /**
     * Creates a LookupResult with a single row or null.
     *
     * @param row The row, can be null
     */
    public LookupResult(@Nullable T row) {
        this(row == null ? Collections.emptyList() : Collections.singletonList(row), null);
    }

    /**
     * Creates a LookupResult with a list of rows.
     *
     * @param rowList The list of rows
     */
    public LookupResult(List<T> rowList) {
        this(rowList, null);
    }

    /**
     * Creates a LookupResult with a list of InternalRows and a converter.
     *
     * @param rowList The list of InternalRows
     * @param rowSerializer The rowSerializer to use for converting InternalRows to POJOs
     */
    @SuppressWarnings("unchecked")
    private LookupResult(List<T> rowList, @Nullable RowSerializer<T> rowSerializer) {
        this.rowList = rowList;
        this.rowSerializer = rowSerializer;
    }

    /**
     * Creates a LookupResult with InternalRows that can be converted to POJOs.
     *
     * @param rowList The list of InternalRows
     * @param converter The converter to use for converting InternalRows to POJOs
     * @param <P> The POJO type
     * @return A new LookupResult that can convert InternalRows to POJOs
     */
    @SuppressWarnings("unchecked")
    public static <P> LookupResult<P> withRowSerializer(
            List<InternalRow> rowList, RowSerializer<P> converter) {
        return new LookupResult<>((List<P>) (Object) rowList, converter);
    }

    /**
     * Creates a LookupResult with InternalRows that can be converted to POJOs.
     *
     * @param row The InternalRow, can be null
     * @param rowSerializer The rowSerializer to use for converting InternalRows to POJOs
     * @param <P> The POJO type
     * @return A new LookupResult that can convert InternalRows to POJOs
     */
    @SuppressWarnings("unchecked")
    public static <P> LookupResult<P> withRowSerializer(
            @Nullable InternalRow row, RowSerializer<P> rowSerializer) {
        List<InternalRow> rowList =
                row == null ? Collections.emptyList() : Collections.singletonList(row);
        return withRowSerializer(rowList, rowSerializer);
    }

    /**
     * Returns the list of rows.
     *
     * @return The list of rows
     */
    public List<T> getRowList() {
        return rowList;
    }

    /**
     * Returns a single row or null if there are no rows. Throws an exception if there are multiple
     * rows.
     *
     * @return The single row or null
     * @throws IllegalStateException if there are multiple rows
     */
    @SuppressWarnings("unchecked")
    public @Nullable T getSingletonRow() {
        if (rowList.isEmpty()) {
            return null;
        } else if (rowList.size() == 1) {
            T row = rowList.get(0);
            // If we have a converter and the row is an InternalRow, convert it
            if (rowSerializer != null && row instanceof InternalRow) {
                return rowSerializer.fromInternalRow((InternalRow) row);
            }
            return row;
        } else {
            throw new IllegalStateException(
                    "Expecting exactly one row, but got: " + rowList.size());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LookupResult lookupResult = (LookupResult) o;
        return Objects.equals(rowList, lookupResult.rowList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowList);
    }

    @Override
    public String toString() {
        return rowList.toString();
    }
}
