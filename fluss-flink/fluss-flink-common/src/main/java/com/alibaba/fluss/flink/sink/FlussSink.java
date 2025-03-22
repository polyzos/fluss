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

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.sink.writer.AppendSinkWriter;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.flink.sink.writer.UpsertSinkWriter;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Flink sink for Fluss with support for any data type. */
public class FlussSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlussSink.class);

    private final SinkWriterBuilder<? extends FlinkSinkWriter> builder;
    private final RowDataConverter<T> converter;
    //    private final FlussSerializationSchema<T> serializationSchema;

    public FlussSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        this.builder = builder;
        this.converter = null; // need to handle this
        //        this.serializationSchema = sinkBuilder.serializationSchema;
    }

    private FlussSink(
            SinkWriterBuilder<? extends FlinkSinkWriter> builder,
            Builder<T> sinkBuilder,
            RowDataConverter<T> converter) {
        this.builder = builder;
        this.converter = converter;
        //        this.serializationSchema = sinkBuilder.serializationSchema;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Deprecated
    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter rowDataWriter = builder.createWriter();
        rowDataWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return new ConvertingSinkWriter<>(rowDataWriter, converter);
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        FlinkSinkWriter rowDataWriter = builder.createWriter();
        rowDataWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return new ConvertingSinkWriter<>(rowDataWriter, converter);
    }

    /** Interface for converting a generic type T to RowData. */
    public interface RowDataConverter<T> extends Serializable {
        RowData convert(T element) throws Exception;
    }

    /** A SinkWriter that converts the input type to RowData before writing. */
    private static class ConvertingSinkWriter<T> implements SinkWriter<T> {
        private final SinkWriter<RowData> rowDataWriter;
        private final RowDataConverter<T> converter;

        public ConvertingSinkWriter(
                SinkWriter<RowData> rowDataWriter, RowDataConverter<T> converter) {
            this.rowDataWriter = rowDataWriter;
            this.converter = converter;
        }

        @Override
        public void write(T element, Context context) throws IOException, InterruptedException {
            RowData rowData;
            try {
                if (!(element instanceof RowData)) {
                    rowData = converter.convert(element);

                } else {
                    rowData = (RowData) element;
                }
                rowDataWriter.write(rowData, context);
            } catch (Exception e) {
                throw new IOException("Error converting element to RowData", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            rowDataWriter.flush(endOfInput);
        }

        @Override
        public void close() throws Exception {
            rowDataWriter.close();
        }
    }

    @Internal
    interface SinkWriterBuilder<W extends FlinkSinkWriter> extends Serializable {
        W createWriter();
    }

    @Internal
    static class AppendSinkWriterBuilder implements SinkWriterBuilder<AppendSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final boolean ignoreDelete;

        public AppendSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                boolean ignoreDelete) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public AppendSinkWriter createWriter() {
            return new AppendSinkWriter(tablePath, flussConfig, tableRowType, ignoreDelete);
        }
    }

    @Internal
    static class UpsertSinkWriterBuilder implements SinkWriterBuilder<UpsertSinkWriter> {

        private static final long serialVersionUID = 1L;

        private final TablePath tablePath;
        private final Configuration flussConfig;
        private final RowType tableRowType;
        private final @Nullable int[] targetColumnIndexes;
        private final boolean ignoreDelete;

        UpsertSinkWriterBuilder(
                TablePath tablePath,
                Configuration flussConfig,
                RowType tableRowType,
                @Nullable int[] targetColumnIndexes,
                boolean ignoreDelete) {
            this.tablePath = tablePath;
            this.flussConfig = flussConfig;
            this.tableRowType = tableRowType;
            this.targetColumnIndexes = targetColumnIndexes;
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public UpsertSinkWriter createWriter() {
            return new UpsertSinkWriter(
                    tablePath, flussConfig, tableRowType, targetColumnIndexes, ignoreDelete);
        }
    }

    /** Builder for {@link FlussSink}. */
    public static class Builder<T> {
        private TablePath tablePath;
        private RowType tableRowType;
        private boolean ignoreDelete = false;
        private int[] targetColumnIndexes = null;
        private boolean isUpsert = false;
        private final Map<String, String> configOptions = new HashMap<>();
        private RowDataConverter<T> converter;
        //        private FlussSerializationSchema<T> serializationSchema;
        private Class<T> pojoClass;

        /** Set the bootstrap server for the sink. */
        public Builder<T> setBootstrapServers(String bootstrapServers) {
            configOptions.put("bootstrap.servers", bootstrapServers);
            return this;
        }

        /** Set the table path for the sink. */
        public Builder<T> setTablePath(TablePath tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        /** Set the table path for the sink. */
        public Builder<T> setTablePath(String database, String table) {
            this.tablePath = new TablePath(database, table);
            return this;
        }

        /** Set the row type for the sink. */
        public Builder<T> setRowType(RowType rowType) {
            this.tableRowType = rowType;
            return this;
        }

        /** Set whether to ignore delete operations. */
        public Builder<T> setIgnoreDelete(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
            return this;
        }

        /** Configure this sink to use upsert semantics. */
        public Builder<T> useUpsert() {
            this.isUpsert = true;
            return this;
        }

        /** Configure this sink to use append-only semantics. */
        public Builder<T> useAppend() {
            this.isUpsert = false;
            return this;
        }

        /** Set a configuration option. */
        public Builder<T> setOption(String key, String value) {
            configOptions.put(key, value);
            return this;
        }

        /** Set multiple configuration options. */
        public Builder<T> setOptions(Map<String, String> options) {
            configOptions.putAll(options);
            return this;
        }

        /** Set a custom converter for transforming elements to RowData. */
        public Builder<T> setConverter(RowDataConverter<T> converter) {
            this.converter = converter;
            return this;
        }

        /** Set the POJO class for automatic schema inference. */
        public Builder<T> setPojoClass(Class<T> pojoClass) {
            this.pojoClass = pojoClass;
            return this;
        }

        //        public Builder<T> setSerializationSchema(FlussSerializationSchema<T>
        // serializationSchema) {
        //            this.serializationSchema = serializationSchema;
        //            return this;
        //        }

        /** Build the FlussSink. */
        public FlussSink<T> build() {
            validateConfiguration();
            System.out.println("Building FlussSink");
            //            this.pojoClass = pojoClass;
            // //serializationSchema.getProducedType().getTypeClass();
            setupConverterAndRowType();

            Configuration flussConfig = Configuration.fromMap(configOptions);

            SinkWriterBuilder<? extends FlinkSinkWriter> writerBuilder;
            if (isUpsert) {
                LOG.info("Using upsert sink");
                writerBuilder =
                        new UpsertSinkWriterBuilder(
                                tablePath,
                                flussConfig,
                                tableRowType,
                                targetColumnIndexes,
                                ignoreDelete);
            } else {
                LOG.info("Using append sink");
                writerBuilder =
                        new AppendSinkWriterBuilder(
                                tablePath, flussConfig, tableRowType, ignoreDelete);
            }

            return new FlussSink<>(writerBuilder, this, converter);
        }

        private void setupConverterAndRowType() {
            // If converter is not provided but we have a POJO class
            System.out.println(converter);
            System.out.println(pojoClass);
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
}
