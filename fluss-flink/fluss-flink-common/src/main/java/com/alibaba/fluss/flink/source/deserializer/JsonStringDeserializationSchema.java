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

import com.alibaba.fluss.record.LogRecord;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.HashMap;
import java.util.Map;

/**
 * A deserialization schema that converts {@link LogRecord} objects to {@link String}
 * representations.
 *
 * <p>This deserializer simply converts the scan record to its string representation using {@code
 * String.valueOf()} method and creates a new String instance from the result.
 *
 * <p>Implementation of {@link FlussDeserializationSchema} for producing String values.
 */
public class JsonStringDeserializationSchema implements FlussDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Object> recordMap = new HashMap<>(4);

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();

        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Deserializes a {@link LogRecord} into a JSON {@link String}.
     *
     * @param record the record to deserialize
     * @return JSON string representation of the scan record
     * @throws Exception if deserialization fails
     */
    @Override
    public String deserialize(LogRecord record) throws Exception {
        recordMap.put("offset", record.logOffset());
        recordMap.put("timestamp", record.timestamp());
        recordMap.put("changeType", record.getChangeType().toString());
        recordMap.put("row", record.getRow().toString());

        return objectMapper.writeValueAsString(recordMap);
    }

    /**
     * Returns the TypeInformation for the produced {@link String} type.
     *
     * @return TypeInformation for String class
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
