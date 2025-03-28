package com.alibaba.fluss.flink.sink;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.RowConverters;
import com.alibaba.fluss.flink.sink.writer.FlinkSinkWriter;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FlussSink<IN> extends FlinkSink<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlussSink.class);

    public FlussSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        super(builder);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
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
        public FlinkSink<T> build() {
            validateConfiguration();
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
}
