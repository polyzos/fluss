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

package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for creating and configuring Fluss sink connectors for Apache Flink.
 *
 * <p>The builder supports automatic schema inference from POJO classes using reflection and
 * provides options for customizing data conversion logic through custom converters.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * FlinkSink<Order> sink = new FlussSinkBuilder<Order>()
 *     .setBootstrapServers("localhost:9092")
 *     .setTablePath("mydb", "orders")
 *     .setPojoClass(Order.class)
 *     .useUpsert()
 *     .build();
 * }</pre>
 *
 * @param <IN> The input type of records to be written to Fluss
 */
public class FlussSinkBuilder<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussSinkBuilder.class);

    private TablePath tablePath;
    private RowType tableRowType;
    private boolean ignoreDelete = false;
    private int[] targetColumnIndexes = null;
    private boolean isUpsert = false;
    private final Map<String, String> configOptions = new HashMap<>();
    private RowDataConverter<IN> converter;
    private Class<IN> pojoClass;

    /** Set the bootstrap server for the sink. */
    public FlussSinkBuilder<IN> setBootstrapServers(String bootstrapServers) {
        configOptions.put("bootstrap.servers", bootstrapServers);
        return this;
    }

    /** Set the table path for the sink. */
    public FlussSinkBuilder<IN> setTablePath(TablePath tablePath) {
        this.tablePath = tablePath;
        return this;
    }

    /** Set the table path for the sink. */
    public FlussSinkBuilder<IN> setTablePath(String database, String table) {
        this.tablePath = new TablePath(database, table);
        return this;
    }

    /** Set the row type for the sink. */
    public FlussSinkBuilder<IN> setRowType(RowType rowType) {
        this.tableRowType = rowType;
        return this;
    }

    /** Set whether to ignore delete operations. */
    public FlussSinkBuilder<IN> setIgnoreDelete(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
        return this;
    }

    /** Configure this sink to use upsert semantics. */
    public FlussSinkBuilder<IN> useUpsert() {
        this.isUpsert = true;
        return this;
    }

    /** Configure this sink to use append-only semantics. */
    public FlussSinkBuilder<IN> useAppend() {
        this.isUpsert = false;
        return this;
    }

    /** Set a configuration option. */
    public FlussSinkBuilder<IN> setOption(String key, String value) {
        configOptions.put(key, value);
        return this;
    }

    /** Set multiple configuration options. */
    public FlussSinkBuilder<IN> setOptions(Map<String, String> options) {
        configOptions.putAll(options);
        return this;
    }

    /** Set a custom converter for transforming elements to RowData. */
    public FlussSinkBuilder<IN> setConverter(RowDataConverter<IN> converter) {
        this.converter = converter;
        return this;
    }

    /** Set the POJO class for automatic schema inference. */
    public FlussSinkBuilder<IN> setPojoClass(Class<IN> pojoClass) {
        this.pojoClass = pojoClass;
        return this;
    }

    //        public Builder<T> setSerializationSchema(FlussSerializationSchema<T>
    // serializationSchema) {
    //            this.serializationSchema = serializationSchema;
    //            return this;
    //        }

    /** Build the FlussSink. */
    public FlinkSink<IN> build() {
        validateConfiguration();
        setupConverterAndRowType();

        Configuration flussConfig = Configuration.fromMap(configOptions);

        FlinkSink.SinkWriterBuilder<? extends FlinkSinkWriter> writerBuilder;

        int numBucket = 0;
        List<String> bucketKeys = null;
        List<String> partitionKeys = null;
        @Nullable DataLakeFormat lakeFormat = DataLakeFormat.PAIMON;
        boolean shuffleByBucketId = false;

        if (isUpsert) {
            LOG.info("Using upsert sink");
            writerBuilder =
                    new FlinkSink.UpsertSinkWriterBuilder(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            targetColumnIndexes,
                            ignoreDelete,
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId);
        } else {
            LOG.info("Using append sink");
            writerBuilder =
                    new FlinkSink.AppendSinkWriterBuilder(
                            tablePath,
                            flussConfig,
                            tableRowType,
                            ignoreDelete,
                            numBucket,
                            bucketKeys,
                            partitionKeys,
                            lakeFormat,
                            shuffleByBucketId);
        }

        return new FlinkSink<>(writerBuilder, converter);
    }

    private void setupConverterAndRowType() {
        // If converter is not provided but we have a POJO class
        if (converter == null && pojoClass != null) {
            // Infer row type if not explicitly set
            if (tableRowType == null) {
                tableRowType = RowConverters.inferRowTypeFromClass(pojoClass);
                LOG.info("Inferred RowType from POJO class: {}", tableRowType);
            }

            // Set up converter using PojoToRowDataConverter
            final RowType finalRowType = tableRowType;
            converter =
                    element -> {
                        if (element == null) {
                            return null;
                        }
                        return RowConverters.convertToRowData(element, finalRowType);
                    };
        } else if (converter == null
                && RowData.class.isAssignableFrom((Class<?>) ((Class) pojoClass))) {
            // For RowData, use identity conversion
            converter = element -> (RowData) element;
        } else if (converter == null) {
            // For any other type without explicit converter, try the generic converter
            final RowType finalRowType = tableRowType;
            converter =
                    element -> {
                        if (element == null) {
                            return null;
                        }
                        return RowConverters.pojoToRowData(element);
                    };
        }
    }

    private void validateConfiguration() {
        if (tablePath == null) {
            throw new IllegalArgumentException("Table path must be specified");
        }

        if (!configOptions.containsKey("bootstrap.servers")) {
            throw new IllegalArgumentException("Bootstrap server must be specified");
        }

        if (tableRowType == null && converter == null && pojoClass == null) {
            throw new IllegalArgumentException(
                    "Either rowType, converter, or pojoClass must be specified");
        }
    }
}
