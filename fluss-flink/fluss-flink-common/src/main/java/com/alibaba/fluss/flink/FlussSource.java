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

package com.alibaba.fluss.flink;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.FlinkSource;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.state.SourceEnumeratorState;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

public class FlussSource<OUT> extends FlinkSource implements ResultTypeQueryable {

    private static final long serialVersionUID = 1L;

    private FlussDeserializationSchema<OUT> deserializationSchema;

    public FlussSource(
            Configuration flussConf,
            TablePath tablePath,
            boolean hasPrimaryKey,
            boolean isPartitioned,
            RowType sourceOutputType,
            @Nullable int[] projectedFields,
            OffsetsInitializer offsetsInitializer,
            long scanPartitionDiscoveryIntervalMs,
            FlussDeserializationSchema<OUT> deserializationSchema,
            boolean streaming) {
        super(
                flussConf,
                tablePath,
                hasPrimaryKey,
                isPartitioned,
                sourceOutputType,
                projectedFields,
                offsetsInitializer,
                scanPartitionDiscoveryIntervalMs,
                deserializationSchema,
                streaming);
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext splitEnumeratorContext) {
        return super.createEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, SourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext splitEnumeratorContext,
            SourceEnumeratorState sourceEnumeratorState) {
        return super.restoreEnumerator(splitEnumeratorContext, sourceEnumeratorState);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // https://alibaba.github.io/fluss-docs/docs/engine-flink/options/#read-options
    public static class Builder<T> {
        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        private Configuration flussConf;
        private TablePath tablePath;
        private boolean hasPrimaryKey;
        private boolean isPartitioned;
        private RowType sourceOutputType;
        private int[] projectedFields;
        private Long scanPartitionDiscoveryIntervalMs;
        private OffsetsInitializer offsetsInitializer;
        private FlussDeserializationSchema<T> deserializationSchema;

        private String bootstrapServers;

        private String database;
        private String tableName;
        private boolean streaming = true;

        public Builder<T> setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder<T> setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder<T> setTable(String table) {
            this.tableName = table;
            return this;
        }

        public Builder<T> setScanPartitionDiscoveryIntervalMs(
                long scanPartitionDiscoveryIntervalMs) {
            this.scanPartitionDiscoveryIntervalMs = scanPartitionDiscoveryIntervalMs;
            return this;
        }

        public Builder<T> setStartingOffsets(OffsetsInitializer offsetsInitializer) {
            this.offsetsInitializer = offsetsInitializer;
            return this;
        }

        public Builder<T> setDeserializationSchema(
                FlussDeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public Builder<T> setIsBatch(boolean isBatch) {
            if (isBatch) {
                this.streaming = false;
            }
            return this;
        }

        public Builder<T> setProjectedFields(int[] projectedFields) {
            this.projectedFields = projectedFields;
            return this;
        }

        public Builder<T> setFlussConfig(Configuration flussConf) {
            this.flussConf = flussConf;
            return this;
        }

        public FlussSource<T> build() {

            Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be empty");
            Objects.requireNonNull(deserializationSchema, "DeserializationSchema must not be null");
            //            Objects.requireNonNull(rowType, "RowType cannot be null");

            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                throw new IllegalArgumentException("bootstrapServers must not be empty");
            }

            if (database == null || database.isEmpty()) {
                throw new IllegalArgumentException("database must not be empty");
            }

            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("tableName must not be empty");
            }

            if (scanPartitionDiscoveryIntervalMs == null) {
                throw new IllegalArgumentException(
                        "scanPartitionDiscoveryIntervalMs must not be null");
            }

            if (offsetsInitializer == null) {
                throw new IllegalArgumentException("offsetsInitializer must not be null");
            }

            if (deserializationSchema == null) {
                throw new IllegalArgumentException("deserializationSchema must not be null");
            }

            if (this.flussConf == null) {
                this.flussConf = new Configuration();
            }

            this.tablePath = new TablePath(this.database, this.tableName);
            this.flussConf.setString("bootstrap.servers", bootstrapServers);

            TableInfo tableInfo;
            try (Connection connection = ConnectionFactory.createConnection(flussConf);
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

            flussConf.addAll(tableInfo.getCustomProperties());
            flussConf.addAll(tableInfo.getProperties());

            isPartitioned = !tableInfo.getPartitionKeys().isEmpty();
            hasPrimaryKey = !tableInfo.getPrimaryKeys().isEmpty();

            sourceOutputType =
                    projectedFields != null
                            ? tableInfo.getRowType().project(projectedFields)
                            : tableInfo.getRowType();

            LOG.info("Creating Fluss Source with Configuration: {}", flussConf);

            return new FlussSource<T>(
                    flussConf,
                    tablePath,
                    hasPrimaryKey,
                    isPartitioned,
                    sourceOutputType,
                    projectedFields,
                    offsetsInitializer,
                    scanPartitionDiscoveryIntervalMs,
                    deserializationSchema,
                    streaming);
        }
    }
}
