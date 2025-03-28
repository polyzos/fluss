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

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * A deserialization schema that converts {@link LogRecord} objects to Flink's {@link RowData}
 * format.
 *
 * <p>This implementation takes a {@link RowType} in its constructor and uses a {@link
 * FlussRowToFlinkRowConverter} to transform Fluss records into Flink's internal row representation.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * RowType rowType = ...; // Define your row type schema
 * RowDataDeserializationSchema schema = new RowDataDeserializationSchema(rowType);
 * FlussSource<RowData> source = FlussSource.builder()
 *     .setDeserializationSchema(schema)
 *     .build();
 * }</pre>
 *
 * @since 0.7
 */
@PublicEvolving
public class RowDataDeserializationSchema implements FlussDeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;

    /**
     * Converter responsible for transforming Fluss row data into Flink's {@link RowData} format.
     * Initialized either in constructor or during {@link #open(InitializationContext)}.
     */
    private FlussRowToFlinkRowConverter converter;

    /**
     * Optional row type provided via constructor. If null, row type will be inferred from context.
     */
    private final RowType rowType;

    /**
     * Creates a new {@link RowDataDeserializationSchema} that will infer the row type from the
     * context during initialization.
     */
    public RowDataDeserializationSchema() {
        this.rowType = null;
    }

    /**
     * Creates a new {@link RowDataDeserializationSchema} with the specified row type.
     *
     * @param rowType The Fluss row type that describes the structure of the input data
     */
    public RowDataDeserializationSchema(RowType rowType) {
        this.rowType = rowType;
        this.converter = new FlussRowToFlinkRowConverter(rowType);
    }

    /**
     * Initializes the deserialization schema.
     *
     * <p>This implementation doesn't require any initialization.
     *
     * @param context Contextual information for initialization
     * @throws Exception if initialization fails
     */
    @Override
    public void open(InitializationContext context) throws Exception {
        if (converter == null) {
            RowType schemaToUse = rowType != null ? rowType : context.getRowSchema();
            this.converter = new FlussRowToFlinkRowConverter(schemaToUse);
        }
    }

    /**
     * Deserializes a {@link LogRecord} into a Flink {@link RowData} object.
     *
     * @param record The Fluss LogRecord to deserialize
     * @return The deserialized RowData
     * @throws Exception If deserialization fails or if the record is not a valid {@link ScanRecord}
     */
    @Override
    public RowData deserialize(LogRecord record) throws Exception {
        if (converter == null) {
            throw new IllegalStateException(
                    "Converter not initialized. The open() method must be called before deserializing records.");
        }
        return converter.toFlinkRowData(record);
    }

    /**
     * Returns the TypeInformation for the produced {@link RowData} type.
     *
     * @return TypeInformation for RowData class
     */
    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(RowData.class);
    }
}
