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

package com.alibaba.fluss.client.row;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.row.InternalRow;

/**
 * Interface for converting between POJOs and {@link InternalRow}.
 *
 * <p>Users can implement this interface to provide custom conversion logic between their POJOs and
 * Fluss's internal row representation.
 *
 * @param <T> The POJO type to convert to/from {@link InternalRow}
 */
@PublicEvolving
public interface RowSerializer<T> {

    /**
     * Converts a POJO to an {@link InternalRow}.
     *
     * @param pojo The POJO to convert
     * @return The converted {@link InternalRow}
     */
    InternalRow toInternalRow(T pojo);

    /**
     * Converts an {@link InternalRow} to a POJO.
     *
     * @param row The InternalRow to convert
     * @return The converted POJO
     */
    T fromInternalRow(InternalRow row);
}
