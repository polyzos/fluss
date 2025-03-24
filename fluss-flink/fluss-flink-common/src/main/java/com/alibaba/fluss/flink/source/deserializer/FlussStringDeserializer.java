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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.client.table.scanner.ScanRecord;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A deserialization schema that converts {@link ScanRecord} objects to {@link String}
 * representations.
 *
 * <p>This deserializer simply converts the scan record to its string representation using {@code
 * String.valueOf()} method and creates a new String instance from the result.
 *
 * <p>Implementation of {@link FlussDeserializationSchema} for producing String values.
 */
public class FlussStringDeserializer implements FlussDeserializationSchema<String> {

    /**
     * Deserializes a {@link ScanRecord} into a {@link String}.
     *
     * @param scanRecord the record to deserialize
     * @return String representation of the scan record
     * @throws Exception if deserialization fails
     */
    @Override
    public String deserialize(ScanRecord scanRecord) throws Exception {
        return new String(String.valueOf(scanRecord));
    }

    /**
     * Returns the TypeInformation for the produced {@link String} type.
     *
     * @return TypeInformation for String class
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
