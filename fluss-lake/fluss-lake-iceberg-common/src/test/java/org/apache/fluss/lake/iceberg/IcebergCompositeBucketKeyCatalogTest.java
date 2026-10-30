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

package org.apache.fluss.lake.iceberg;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.lakestorage.TestingLakeCatalogContext;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for creating Iceberg tables with a multi-column bucket key in {@link IcebergLakeCatalog}.
 */
class IcebergCompositeBucketKeyCatalogTest {

    private static final String DATABASE = "composite_db";

    @TempDir private File tempWarehouseDir;

    private IcebergLakeCatalog flussIcebergCatalog;

    @BeforeEach
    void setupCatalog() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", tempWarehouseDir.toURI().toString());
        configuration.setString("catalog-impl", "org.apache.iceberg.inmemory.InMemoryCatalog");
        configuration.setString("name", "fluss_test_catalog");
        this.flussIcebergCatalog = new IcebergLakeCatalog(configuration);
    }

    @Test
    void testBucketKeyAsStrictPrefixOfPrimaryKeyIsBucketedByFirstColumn() {
        TablePath tablePath = TablePath.of(DATABASE, "order_items");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(orderItemsSchema(DataTypes.BIGINT()))
                        .distributedBy(4, "order_id", "tenant_id")
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, 2)
                        .build();

        flussIcebergCatalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());

        Table table = loadTable(tablePath);
        List<PartitionField> fields = table.spec().fields();
        assertThat(fields).hasSize(1);
        assertThat(fields.get(0).transform().toString()).isEqualTo("bucket[4]");
        assertThat(table.schema().findColumnName(fields.get(0).sourceId())).isEqualTo("order_id");
        // the identifier fields (used for equality deletes) are the whole primary key
        assertThat(table.schema().identifierFieldNames())
                .containsExactlyInAnyOrder("order_id", "tenant_id", "item_id");

        // re-creating the same table is idempotent
        flussIcebergCatalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());
        assertThat(loadTable(tablePath).spec()).isEqualTo(table.spec());
    }

    @Test
    void testPartitionedTableIsBucketedByFirstBucketKeyColumn() {
        Schema schema =
                Schema.newBuilder()
                        .column("dt", DataTypes.STRING())
                        .column("order_id", DataTypes.BIGINT())
                        .column("tenant_id", DataTypes.BIGINT())
                        .column("item_id", DataTypes.BIGINT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("dt", "order_id", "tenant_id", "item_id")
                        .build();
        TablePath tablePath = TablePath.of(DATABASE, "partitioned_order_items");
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .partitionedBy("dt")
                        .distributedBy(3, "order_id", "tenant_id")
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, 2)
                        .build();

        flussIcebergCatalog.createTable(tablePath, descriptor, new TestingLakeCatalogContext());

        Table table = loadTable(tablePath);
        List<PartitionField> fields = table.spec().fields();
        assertThat(fields).hasSize(2);
        assertThat(fields.get(0).transform().isIdentity()).isTrue();
        assertThat(table.schema().findColumnName(fields.get(0).sourceId())).isEqualTo("dt");
        assertThat(fields.get(1).transform().toString()).isEqualTo("bucket[3]");
        assertThat(table.schema().findColumnName(fields.get(1).sourceId())).isEqualTo("order_id");
    }

    @Test
    void testRejectMultiColumnBucketKeyOnLegacyKvFormat() {
        // no kv format version means a legacy (version 1) table
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(orderItemsSchema(DataTypes.BIGINT()))
                        .distributedBy(4, "order_id", "tenant_id")
                        .build();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        TablePath.of(DATABASE, "legacy_order_items"),
                                        descriptor,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("must use kv format version 2");
    }

    @Test
    void testRejectMultiColumnBucketKeyNotAPrefixOfPrimaryKey() {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(orderItemsSchema(DataTypes.BIGINT()))
                        .distributedBy(4, "tenant_id", "order_id")
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, 2)
                        .build();

        assertThatThrownBy(
                        () ->
                                flussIcebergCatalog.createTable(
                                        TablePath.of(DATABASE, "unordered_order_items"),
                                        descriptor,
                                        new TestingLakeCatalogContext()))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining("must be a strict prefix of the primary key");
    }

    @Test
    void testRejectBucketKeyTypeIcebergCannotBucket() {
        // Bucket keys of primary key tables are part of the primary key, which Iceberg already
        // rejects as identifier fields for these types, so this only applies to log tables.
        for (DataType keyType : new DataType[] {DataTypes.FLOAT(), DataTypes.DOUBLE()}) {
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("score", keyType)
                                            .column("name", DataTypes.STRING())
                                            .build())
                            .distributedBy(4, "score")
                            .build();

            assertThatThrownBy(
                            () ->
                                    flussIcebergCatalog.createTable(
                                            TablePath.of(
                                                    DATABASE,
                                                    "log_" + keyType.getTypeRoot().name()),
                                            descriptor,
                                            new TestingLakeCatalogContext()))
                    .isInstanceOf(InvalidTableException.class)
                    .hasMessageContaining("column `score`")
                    .hasMessageContaining("can't be bucketed by Iceberg");
        }
    }

    private static Schema orderItemsSchema(DataType orderIdType) {
        return Schema.newBuilder()
                .column("order_id", orderIdType)
                .column("tenant_id", DataTypes.BIGINT())
                .column("item_id", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .primaryKey("order_id", "tenant_id", "item_id")
                .build();
    }

    private Table loadTable(TablePath tablePath) {
        return flussIcebergCatalog
                .getIcebergCatalog()
                .loadTable(
                        TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName()));
    }
}
