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

package com.alibaba.fluss.flink.row;

import com.alibaba.fluss.row.InternalRow;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

public class RowWithOp {
    /** The internal row data. */
    private final InternalRow row;

    @Nullable private final OperationType opType;

    /**
     * Constructs a {@code RowWithOp} with the specified internal row and a default operation kind.
     *
     * @param row the internal row data
     */
    public RowWithOp(InternalRow row, @Nullable OperationType opType) {
        this.row = checkNotNull(row, "row cannot be null");
        this.opType = checkNotNull(opType, "opType cannot be null");
    }

    /**
     * Returns the internal row data.
     *
     * @return the internal row
     */
    public InternalRow getRow() {
        return row;
    }

    public OperationType getOperationType() {
        return opType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowWithOp rowWithOp = (RowWithOp) o;
        return Objects.equals(row, rowWithOp.row) && opType == rowWithOp.opType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(row, opType);
    }
}
