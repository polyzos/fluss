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

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** Flink sink for Fluss. */
class FlinkSink<IN> implements Sink<IN> {

    private static final long serialVersionUID = 1L;

    private final SinkWriterBuilder<? extends FlinkSinkWriter> builder;
    private final RowDataConverter<IN> converter;

    public FlinkSink(SinkWriterBuilder<? extends FlinkSinkWriter> builder) {
        this.builder = builder;
        this.converter = element -> (RowData) element;
    }

    private FlinkSink(
            SinkWriterBuilder<? extends FlinkSinkWriter> builder, RowDataConverter<IN> converter) {
        this.builder = builder;
        this.converter = converter;
    }

    @Deprecated
    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        FlinkSinkWriter rowDataWriter = builder.createWriter();
        rowDataWriter.initialize(InternalSinkWriterMetricGroup.wrap(context.metricGroup()));
        return new ConvertingSinkWriter<>(rowDataWriter, converter);
    }

    @Override
    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
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
}
