#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Standalone HBase (master, region server and ZooKeeper in one JVM) for
# HBaseStorageHandlerIntegrationSuite. There is no official multi-arch HBase image,
# so the suite builds this one when it is missing. To build it by hand:
#
#   docker build -t spark-test-hbase:2.6.3 \
#     connector/docker-integration-tests/src/test/resources/hbase

FROM eclipse-temurin:11-jre

ARG HBASE_VERSION=2.6.3
ARG HBASE_MIRROR=https://archive.apache.org/dist/hbase

ENV HBASE_HOME=/opt/hbase

RUN set -eux; \
    if ! command -v curl > /dev/null; then \
      apt-get update; \
      apt-get install -y --no-install-recommends curl ca-certificates; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    mkdir -p "${HBASE_HOME}" /data; \
    curl -fsSL "${HBASE_MIRROR}/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz" \
      | tar -xz --strip-components=1 -C "${HBASE_HOME}"; \
    rm -rf "${HBASE_HOME}/docs"

COPY hbase-docker-entrypoint.sh /hbase-docker-entrypoint.sh

ENTRYPOINT ["bash", "/hbase-docker-entrypoint.sh"]
