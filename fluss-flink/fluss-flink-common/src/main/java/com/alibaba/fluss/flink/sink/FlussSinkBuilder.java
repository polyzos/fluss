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

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import com.alibaba.fluss.utils.Preconditions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

    private String bootstrapServers;
    private String database;
    private String tableName;
    private RowType tableRowType;
    private boolean ignoreDelete;
    private int[] targetColumnIndexes;
    private boolean isUpsert;
    private final Map<String, String> configOptions = new HashMap<>();
    private RowDataConverter<IN> converter;
    private Class<IN> inputType;
    private boolean shuffleByBucketId = true;
    private @Nullable DataLakeFormat lakeFormat;

    /** Set the bootstrap server for the sink. */
    public FlussSinkBuilder<IN> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /** Set the database for the sink. */
    public FlussSinkBuilder<IN> setDatabase(String database) {
        this.database = database;
        return this;
    }

    /** Set the table name for the sink. */
    public FlussSinkBuilder<IN> setTable(String table) {
        this.tableName = table;
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

    /** Set target column indexes. */
    public FlussSinkBuilder<IN> setTargetColumnIndexes(int[] targetColumnIndexes) {
        this.targetColumnIndexes = targetColumnIndexes;
        return this;
    }

    /** Set target column indexes. */
    public FlussSinkBuilder<IN> setDataLakeFormat(DataLakeFormat lakeFormat) {
        this.lakeFormat = lakeFormat;
        return this;
    }

    /** Set shuffle by bucket id. */
    public FlussSinkBuilder<IN> setShuffleByBucketId(boolean shuffleByBucketId) {
        if (!shuffleByBucketId) {
            this.shuffleByBucketId = false;
        }
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
    public FlussSinkBuilder<IN> setInputType(Class<IN> inputType) {
        this.inputType = inputType;
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

        TablePath tablePath = new TablePath(database, tableName);
        flussConfig.setString("bootstrap.servers", bootstrapServers);

        System.out.println(tablePath);
        TableInfo tableInfo;
        try (Connection connection = ConnectionFactory.createConnection(flussConfig);
             Admin admin = connection.getAdmin()) {
            try {
                tableInfo = admin.getTableInfo(tablePath).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while getting table info", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Failed to get table info", e);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize FlussSource admin connection: " + e.getMessage(), e);
        }
        int numBucket = tableInfo.getNumBuckets();
        List<String> bucketKeys = tableInfo.getBucketKeys();
        List<String> partitionKeys = tableInfo.getPartitionKeys();

        System.out.println(tableInfo);
        System.out.println(numBucket);
        System.out.println(bucketKeys);
        System.out.println(partitionKeys);

        System.out.println();
        System.out.println(ignoreDelete);
        System.out.println(targetColumnIndexes);
        System.out.println(isUpsert);;


        System.out.println();

        System.out.println("Starting Fluss Sink with configuration: " + flussConfig);
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
        if (converter == null && inputType != null) {
            // Infer row type if not explicitly set
            if (tableRowType == null) {
                tableRowType = RowConverters.inferRowTypeFromClass(inputType);
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
                && RowData.class.isAssignableFrom((Class<?>) ((Class) inputType))) {
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
        System.out.println(database);
        System.out.println("edw -> " + database.isEmpty());
        Preconditions.checkNotNull(bootstrapServers, "BootstrapServers is required but not provided.");

        Preconditions.checkNotNull(database, "DatabaseName is required but not provided.");
        Preconditions.checkArgument(!database.isEmpty(), "Database cannot be empty.");

        Preconditions.checkNotNull(tableName, "Table name is required but not provided.");
        Preconditions.checkArgument(!tableName.isEmpty(), "Table name cannot be empty.");

        Preconditions.checkArgument(tableRowType != null || converter != null || inputType != null, "Either rowType, converter, or inputType must be specified.");
    }
}
