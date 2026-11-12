#!/usr/bin/env bash

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

# Runs HBase in standalone mode in the foreground.
#
# HBase clients look up the master and region server addresses (host and port) in
# ZooKeeper, so the container must publish every port under the same number on the host
# and advertise a host name that works both inside the container and on the Docker host.
# The ports come from the environment so the test can pick free ones.

set -euo pipefail

ZK_PORT="${HBASE_ZK_PORT:-2181}"
MASTER_PORT="${HBASE_MASTER_PORT:-16000}"
REGIONSERVER_PORT="${HBASE_REGIONSERVER_PORT:-16020}"
ADVERTISED_HOST="${HBASE_ADVERTISED_HOST:-localhost}"
export HBASE_HEAPSIZE="${HBASE_HEAPSIZE:-1g}"

property() {
  printf '  <property><name>%s</name><value>%s</value></property>\n' "$1" "$2"
}

{
  echo '<?xml version="1.0"?>'
  echo '<configuration>'
  property hbase.cluster.distributed false
  property hbase.rootdir file:///data/hbase
  property hbase.tmp.dir /data/tmp
  property hbase.unsafe.stream.capability.enforce false
  # The in-process ZooKeeper binds to the loopback address unless told otherwise.
  property hbase.zookeeper.quorum localhost
  property hbase.zookeeper.property.clientPort "${ZK_PORT}"
  property hbase.zookeeper.property.clientPortAddress 0.0.0.0
  property hbase.zookeeper.property.dataDir /data/zookeeper
  property hbase.localcluster.assign.random.ports false
  property hbase.master.hostname "${ADVERTISED_HOST}"
  property hbase.master.ipc.address 0.0.0.0
  property hbase.master.port "${MASTER_PORT}"
  property hbase.master.info.port -1
  property hbase.unsafe.regionserver.hostname "${ADVERTISED_HOST}"
  property hbase.regionserver.ipc.address 0.0.0.0
  property hbase.regionserver.port "${REGIONSERVER_PORT}"
  property hbase.regionserver.info.port -1
  echo '</configuration>'
} > "${HBASE_HOME}/conf/hbase-site.xml"

exec "${HBASE_HOME}/bin/hbase" master start
