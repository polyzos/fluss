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

# `apache/fluss-quickstart-flink` Docker image

This directory contains the `Dockerfile` for the `apache/fluss-quickstart-flink`
image referenced by the Fluss [Quickstart guide](../../website/docs/quickstart/flink.md).

The image is based on `flink:<FLINK_VERSION>-java17` and bundles:

- The Fluss Flink connector (`fluss-flink-<FLINK_VERSION>`)
- The Fluss S3 filesystem plugin (`fluss-fs-s3`)
- [Flink Faker](https://github.com/knaufk/flink-faker) (demo data generation)
- The Flink Prometheus metrics reporter (available on the classpath, disabled by default)

## Why is this built manually?

The Dockerfile downloads the Fluss jars from a Maven repository at build time.
Fluss does **not** currently publish snapshots to the ASF snapshot repository,
so this image cannot be built from `main` by CI. It is therefore built and
pushed manually by the release manager using a released version (or a staged
release-candidate).

See the [release guide](../../website/community/how-to-release/creating-a-fluss-release.mdx)
for the full workflow.

## Build

### From a released version (artifacts on Maven Central)

```bash
docker buildx build --push --platform linux/arm64/v8,linux/amd64 \
    --build-arg FLINK_VERSION=2.2 \
    --build-arg FLUSS_VERSION=<RELEASE_VERSION> \
    --tag apache/fluss-quickstart-flink:2.2-<RELEASE_VERSION> \
    .
```

### From a staged release candidate (artifacts on the Apache Nexus staging repo)

Use the staging repository URL from the release candidate (see the release
guide, "Deploy to the Nexus staging repository"):

```bash
docker buildx build --push --platform linux/arm64/v8,linux/amd64 \
    --build-arg FLINK_VERSION=2.2 \
    --build-arg FLUSS_VERSION=<RELEASE_VERSION> \
    --build-arg FLUSS_MAVEN_REPO_URL=https://repository.apache.org/content/repositories/orgapachefluss-<STAGING_ID>/ \
    --tag apache/fluss-quickstart-flink:2.2-<RELEASE_VERSION>-rc<RC_NUM> \
    .
```

## Build arguments

| Arg                     | Required | Default                                | Description                                                                  |
|-------------------------|----------|----------------------------------------|------------------------------------------------------------------------------|
| `FLINK_VERSION`         | No       | `2.2`                                  | Flink major/minor version for the base image and the `fluss-flink-*` jar.    |
| `FLUSS_VERSION`         | **Yes**  | –                                      | Fluss version used to resolve the `fluss-flink-*` and `fluss-fs-s3` jars.    |
| `FLUSS_MAVEN_REPO_URL`  | No       | `https://repo1.maven.org/maven2`       | Maven repository to download the Fluss jars from.                            |

The build fails fast if `FLUSS_VERSION` is not provided.
