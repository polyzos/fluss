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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.ProduceLogResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.config.ConfigOptions.KV_FORMAT_VERSION_2;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link ReplicaManager} rejects writes to Iceberg-format tables whose bucket keys are
 * not supported. Such tables can only exist if they were created before the bucket key validation
 * at table creation existed, so they are registered directly in ZooKeeper here.
 */
class IcebergBucketKeyWriteValidationTest extends ReplicaTestBase {

    private static final short PUT_KV_VERSION = 1;

    @Test
    void testRejectAppendToLogTableWithMultiColumnBucketKey() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "iceberg_log_table");
        long tableId =
                registerTableInZkClient(
                        tablePath,
                        DATA1_SCHEMA,
                        2998234L,
                        Arrays.asList("a", "b"),
                        icebergProperties(null));
        TableBucket tableBucket = new TableBucket(tableId, 0);
        makeLeader(tablePath, tableBucket);

        ProduceLogResultForBucket result = appendDataTo(tableBucket);
        assertThat(result.getErrorCode()).isEqualTo(Errors.INVALID_TABLE_EXCEPTION.code());
        assertThat(result.getErrorMessage()).contains("the table is a log table");
    }

    @Test
    void testRejectPutToPrimaryKeyTableWithCompositeDefaultBucketKey() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();
        TablePath tablePath = TablePath.of("test_db", "iceberg_pk_table");
        long tableId =
                registerTableInZkClient(
                        tablePath,
                        schema,
                        2998235L,
                        Arrays.asList("a", "b"),
                        icebergProperties(KV_FORMAT_VERSION_2));
        TableBucket tableBucket = new TableBucket(tableId, 0);
        makeKvTableAsLeader(tableId, tablePath, tableBucket.getBucket());

        CompletableFuture<List<PutKvResultForBucket>> future = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tableBucket, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                MergeMode.DEFAULT,
                PUT_KV_VERSION,
                future::complete);
        PutKvResultForBucket result = future.get().get(0);
        assertThat(result.getErrorCode()).isEqualTo(Errors.INVALID_TABLE_EXCEPTION.code());
        assertThat(result.getErrorMessage()).contains("must be a strict prefix of the primary key");
    }

    @Test
    void testAllowAppendWithMultiColumnBucketKeyWithoutIceberg() throws Exception {
        TablePath tablePath = TablePath.of("test_db", "plain_log_table");
        long tableId =
                registerTableInZkClient(
                        tablePath,
                        DATA1_SCHEMA,
                        2998236L,
                        Arrays.asList("a", "b"),
                        Collections.emptyMap());
        TableBucket tableBucket = new TableBucket(tableId, 0);
        makeLeader(tablePath, tableBucket);

        assertThat(appendDataTo(tableBucket))
                .isEqualTo(new ProduceLogResultForBucket(tableBucket, 0, 10L));
    }

    private ProduceLogResultForBucket appendDataTo(TableBucket tableBucket) throws Exception {
        CompletableFuture<List<ProduceLogResultForBucket>> future = new CompletableFuture<>();
        replicaManager.appendRecordsToLog(
                20000,
                1,
                Collections.singletonMap(tableBucket, genMemoryLogRecordsByObject(DATA1)),
                null,
                future::complete);
        return future.get().get(0);
    }

    private void makeLeader(TablePath tablePath, TableBucket tableBucket) throws Exception {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> future =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(tablePath),
                                tableBucket,
                                Collections.singletonList(TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH,
                                        Collections.singletonList(TABLET_SERVER_ID),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH),
                                TEST_ROUTING_BUCKET_COUNT,
                                0L)),
                future::complete);
        assertThat(future.get()).containsOnly(new NotifyLeaderAndIsrResultForBucket(tableBucket));
    }

    private static Map<String, String> icebergProperties(Integer kvFormatVersion) {
        Map<String, String> properties = new HashMap<>();
        properties.put(
                ConfigOptions.TABLE_DATALAKE_FORMAT.key(), DataLakeFormat.ICEBERG.toString());
        if (kvFormatVersion != null) {
            properties.put(
                    ConfigOptions.TABLE_KV_FORMAT_VERSION.key(), String.valueOf(kvFormatVersion));
        }
        return properties;
    }
}
