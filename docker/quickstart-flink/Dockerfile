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

# ARG before FROM is consumed only by FROM; redeclare after FROM to use in RUN.
ARG FLINK_VERSION=2.2

FROM flink:${FLINK_VERSION}-java17

ARG FLINK_VERSION=2.2
ARG FLUSS_VERSION
ARG FLUSS_MAVEN_REPO_URL=https://repo1.maven.org/maven2

# Pre-install dependencies into Flink lib so that quickstart users do not need to
# download any jars manually:
#   - Fluss Flink connector
#   - Fluss S3 filesystem plugin (needed for remote tiered storage in quickstart)
#   - Flink Faker connector (for generating demo data in quickstart examples)
#   - Flink Prometheus metrics reporter (placed in lib/ so it is on the classpath;
#     it acts as a reporter only when explicitly configured via metrics.reporter.*)
RUN set -ex; \
    # Fluss JARs are downloaded from the official Apache Maven repository (Maven Central
    # for releases, ASF Snapshot repo for snapshots). Integrity is ensured by the TLS
    # connection to the repository host and Maven Central's GPG-signed artifacts.
    # SHA-256 pinning is not used here because the FLUSS_VERSION build-arg varies per
    # release; instead, trust is delegated to the repository's transport security.
    curl -fL -o /opt/flink/lib/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/lib/fluss-fs-s3-${FLUSS_VERSION}.jar \
        "${FLUSS_MAVEN_REPO_URL}/org/apache/fluss/fluss-fs-s3/${FLUSS_VERSION}/fluss-fs-s3-${FLUSS_VERSION}.jar"; \
    curl -fL -o /opt/flink/lib/flink-faker-0.5.3.jar \
        "https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar"; \
    echo "d5015905354f3580cd9a1e984501cbfca843a4ce8e9e7f6c7ff81f5abf9c5a7e  /opt/flink/lib/flink-faker-0.5.3.jar" | sha256sum -c -; \
    cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/lib/
