<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Fluss Quickstart Flink Docker

This directory contains the Dockerfile for the `apache/fluss-quickstart-flink` image used by the Fluss quickstart documentation.

The image is based on the official Flink Docker image and pre-installs the jars needed by the Fluss quickstart guides:

- `fluss-flink`
- `fluss-fs-s3`
- `flink-faker`
- Flink Prometheus metrics reporter
- `fluss-flink-tiering`

Lakehouse-specific dependencies are pre-bundled in separate directories inside the image:

- `/opt/flink/paimon`
- `/opt/flink/iceberg`

Use the bundled init scripts to activate them before starting Flink:

- `/opt/flink/init_paimon.sh`
- `/opt/flink/init_iceberg.sh`

## Build the image

Build a release image from Maven Central:

```bash
docker buildx build \
  --platform linux/arm64/v8,linux/amd64 \
  --build-arg FLINK_VERSION=1.20 \
  --build-arg FLUSS_VERSION=<FLUSS_VERSION> \
  --tag apache/fluss-quickstart-flink:1.20-<FLUSS_VERSION> \
  .
```

For example:

```bash
docker buildx build \
  --platform linux/arm64/v8,linux/amd64 \
  --build-arg FLINK_VERSION=1.20 \
  --build-arg FLUSS_VERSION=0.9.0-incubating \
  --tag apache/fluss-quickstart-flink:1.20-0.9.0-incubating \
  .
```

## Build an RC or snapshot image

When building against staged or snapshot Fluss artifacts, override `FLUSS_MAVEN_REPO_URL`:

```bash
docker buildx build \
  --platform linux/arm64/v8,linux/amd64 \
  --build-arg FLINK_VERSION=1.20 \
  --build-arg FLUSS_VERSION=<FLUSS_VERSION> \
  --build-arg FLUSS_MAVEN_REPO_URL=https://repository.apache.org/content/repositories/orgapachefluss-<STAGING_ID> \
  --tag apache/fluss-quickstart-flink:1.20-<FLUSS_VERSION> \
  .
```
