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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;
import java.util.List;

/** Result of FullKvScanRequest for each table bucket. */
public class FullKvScanResultForBucket extends ResultForBucket {

    @Nullable private final List<Tuple2<byte[], byte[]>> keyValues;

    public FullKvScanResultForBucket(
            TableBucket tableBucket, List<Tuple2<byte[], byte[]>> keyValues) {
        super(tableBucket, ApiError.NONE);
        this.keyValues = keyValues;
    }

    public FullKvScanResultForBucket(TableBucket tableBucket, ApiError error) {
        super(tableBucket, error);
        this.keyValues = null;
    }

    @Nullable
    public List<Tuple2<byte[], byte[]>> getKeyValues() {
        return keyValues;
    }
}
