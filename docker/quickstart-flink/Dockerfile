#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG FLINK_VERSION=1.20

FROM flink:${FLINK_VERSION}-java17

ARG FLINK_VERSION=1.20
ARG FLUSS_VERSION
ARG FLUSS_MAVEN_REPO_URL=https://repo1.maven.org/maven2
ARG PAIMON_VERSION=1.3.1
ARG ICEBERG_VERSION=1.10.1
ARG POSTGRES_VERSION=42.7.4
ARG HADOOP_VERSION=3.3.5

COPY bin/init_paimon.sh /opt/flink/init_paimon.sh
COPY bin/init_iceberg.sh /opt/flink/init_iceberg.sh
COPY bin/sql-client /opt/flink/bin/sql-client
COPY sql/sql-client.sql /opt/flink/sql-client/sql-client.sql

# Pre-install the dependencies used by Fluss quickstart guides so users do not
# need to download jars manually before starting the demo environment.
RUN set -ex; \
    mkdir -p /opt/flink/paimon /opt/flink/iceberg; \
    curl -fL -o /opt/flink/lib/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/lib/fluss-fs-s3-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/lib/flink-faker-0.5.3.jar \
        "https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar"; \
    echo "d5015905354f3580cd9a1e984501cbfca843a4ce8e9e7f6c7ff81f5abf9c5a7e  /opt/flink/lib/flink-faker-0.5.3.jar" | sha256sum -c -; \
    curl -fL -o /opt/flink/opt/fluss-flink-tiering-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-flink-tiering/${FLUSS_VERSION}/fluss-flink-tiering-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/paimon/paimon-flink-${FLINK_VERSION}-${PAIMON_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-${FLINK_VERSION}/${PAIMON_VERSION}/paimon-flink-${FLINK_VERSION}-${PAIMON_VERSION}.jar"; \
    curl -fL -o /opt/flink/paimon/fluss-lake-paimon-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-lake-paimon/${FLUSS_VERSION}/fluss-lake-paimon-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/paimon/paimon-bundle-${PAIMON_VERSION}.jar \
        "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/${PAIMON_VERSION}/paimon-bundle-${PAIMON_VERSION}.jar"; \
    curl -fL -o /opt/flink/paimon/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
        "https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"; \
    curl -fL -o /opt/flink/paimon/paimon-s3-${PAIMON_VERSION}.jar \
        "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/${PAIMON_VERSION}/paimon-s3-${PAIMON_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_VERSION}-${ICEBERG_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/fluss-lake-iceberg-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-lake-iceberg/${FLUSS_VERSION}/fluss-lake-iceberg-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/iceberg-aws-${ICEBERG_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/${ICEBERG_VERSION}/iceberg-aws-${ICEBERG_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/postgresql-${POSTGRES_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/hadoop-client-api-${HADOOP_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar"; \
    curl -fL -o /opt/flink/iceberg/hadoop-client-runtime-${HADOOP_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"; \
    cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/lib/; \
    chmod +x /opt/flink/init_paimon.sh /opt/flink/init_iceberg.sh /opt/flink/bin/sql-client
