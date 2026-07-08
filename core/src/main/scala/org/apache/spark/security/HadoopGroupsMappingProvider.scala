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

package org.apache.spark.security

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.security.Groups

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging

/**
 * This class is responsible for getting the groups for a particular user via the Hadoop group
 * mapping service configured by `hadoop.security.group.mapping`, including composite or LDAP
 * based mappings configured via `hadoop.security.group.mapping.providers`, so that Spark ACLs
 * resolve groups from the same source as HDFS and YARN. The Hadoop configuration is loaded from
 * the usual sources (e.g. `HADOOP_CONF_DIR` entries on the classpath) and can be extended or
 * overridden with `spark.hadoop.*` properties. Lookups are cached by the Hadoop `Groups` service
 * according to `hadoop.security.groups.cache.secs`.
 */
private[spark] class HadoopGroupsMappingProvider extends GroupMappingServiceProvider
  with Logging {

  // A dedicated Groups instance (rather than Groups.getUserToGroupsMappingService) so that the
  // mapping honors this process' SparkConf overrides regardless of whether the shared service
  // was already initialized elsewhere (e.g. by UserGroupInformation) with a different config.
  private lazy val groups = new Groups(SparkHadoopUtil.get.newConfiguration(new SparkConf()))

  override def getGroups(username: String): Set[String] = {
    val userGroups = try {
      groups.getGroupsSet(username).asScala.toSet
    } catch {
      // Groups throws IOException when the user is unknown or has no groups
      case NonFatal(e) =>
        logDebug(s"Unable to resolve groups for user: $username", e)
        Set.empty[String]
    }
    logDebug("User: " + username + " Groups: " + userGroups.mkString(","))
    userGroups
  }
}
