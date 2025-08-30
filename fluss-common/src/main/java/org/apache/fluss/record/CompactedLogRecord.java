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

package org.apache.fluss.record;

import org.apache.fluss.row.InternalRow;

/**
 * This class is an immutable log record  for @CompactedRow and can be directly persisted. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Attributes => Int8
 *   <li>Value => {@link InternalRow}
 * </ul>
 *
 * <p>The current record attributes are depicted below:
 *
 * <p>----------- | ChangeType (0-3) | Unused (4-7) |---------------
 *
 * <p>The offset compute the difference relative to the base offset and of the batch that this
 * record is contained in.
 *
 * @since 0.8
 */
public class CompactedLogRecord implements LogRecord {
    @Override
    public long logOffset() {
        return 0;
    }

    @Override
    public long timestamp() {
        return 0;
    }

    @Override
    public ChangeType getChangeType() {
        return null;
    }

    @Override
    public InternalRow getRow() {
        return null;
    }
}
