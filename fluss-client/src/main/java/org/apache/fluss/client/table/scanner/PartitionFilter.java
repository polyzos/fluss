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

import javax.annotation.Nullable;

/**
 * A simple filter that narrows scans to a specific partition.
 *
 * <p>This is an initial, minimal filter abstraction to enable partition pruning for client-side
 * scans. For now, only equality on a concrete partition name is supported. Row-level filtering is
 * intentionally not supported here.
 */
public final class PartitionFilter {

    @Nullable private final String partitionName;

    private PartitionFilter(@Nullable String partitionName) {
        this.partitionName = partitionName;
    }

    /** Creates a filter that limits the scan to the given partition name. */
    public static PartitionFilter ofPartitionName(String partitionName) {
        return new PartitionFilter(partitionName);
    }

    /** Returns the partition name to filter by, or null if none. */
    @Nullable
    public String getPartitionName() {
        return partitionName;
    }
}
