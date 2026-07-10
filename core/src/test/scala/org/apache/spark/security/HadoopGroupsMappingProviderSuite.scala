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

import java.util.{ArrayList => JArrayList, Arrays, List => JList}

import org.apache.spark.SparkFunSuite

class HadoopGroupsMappingProviderSuite extends SparkFunSuite {

  private val mappingKey = "spark.hadoop.hadoop.security.group.mapping"

  private def withTestGroupMapping(f: => Unit): Unit = {
    System.setProperty(mappingKey, classOf[TestHadoopGroupsMapping].getName)
    try {
      f
    } finally {
      System.clearProperty(mappingKey)
    }
  }

  test("resolves groups via the configured Hadoop group mapping service") {
    withTestGroupMapping {
      val provider = new HadoopGroupsMappingProvider()
      assert(provider.getGroups("alice") === Set("wheel", "analysts"))
    }
  }

  test("returns an empty set for users without groups") {
    withTestGroupMapping {
      val provider = new HadoopGroupsMappingProvider()
      assert(provider.getGroups("bob") === Set.empty)
    }
  }
}

/**
 * A deterministic Hadoop group mapping used instead of the OS/LDAP backed defaults.
 * Must be a public top-level class so that Hadoop can instantiate it reflectively.
 */
class TestHadoopGroupsMapping extends org.apache.hadoop.security.GroupMappingServiceProvider {

  override def getGroups(user: String): JList[String] = {
    if (user == "alice") {
      Arrays.asList("wheel", "analysts")
    } else {
      new JArrayList[String]()
    }
  }

  override def cacheGroupsRefresh(): Unit = {}

  override def cacheGroupsAdd(groups: JList[String]): Unit = {}
}
