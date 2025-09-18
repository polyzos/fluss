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

package org.apache.fluss.client.lookup;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;

/** IT cases for primary-key point lookups via Lookuper. */
class PrimaryKeyLookuperITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initClusterConfig())
                    .build();

    private Connection conn;
    private Admin admin;
    private Configuration clientConf;

    private static Configuration initClusterConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.MAX_PARTITION_NUM, 10);
        conf.set(ConfigOptions.MAX_BUCKET_NUM, 30);
        return conf;
    }

    @BeforeEach
    void setUp() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void testPointLookupNonPartitioned() throws Exception {
        TablePath tablePath = TablePath.of("pk_lookup", "non_partitioned_pk");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a")
                        .build();

        TableDescriptor desc =
                TableDescriptor.builder().schema(schema).distributedBy(3, "a").build();

        // create db/table
        admin.createDatabase(tablePath.getDatabaseName(), DatabaseDescriptor.EMPTY, true).get();
        admin.createTable(tablePath, desc, false).get();

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsert = table.newUpsert().createWriter();
            upsert.upsert(row(1, "v1"));
            upsert.upsert(row(2, "v2"));
            upsert.flush();

            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow r1 = lookuper.lookup(row(1)).get().getSingletonRow();
            InternalRow r2 = lookuper.lookup(row(2)).get().getSingletonRow();
            InternalRow r3 = lookuper.lookup(row(3)).get().getSingletonRow();

            assertThatRow(r1).withSchema(schema.getRowType()).isEqualTo(row(1, "v1"));
            assertThatRow(r2).withSchema(schema.getRowType()).isEqualTo(row(2, "v2"));
            assertThatRow(r3).isNull();
        }
    }
}
