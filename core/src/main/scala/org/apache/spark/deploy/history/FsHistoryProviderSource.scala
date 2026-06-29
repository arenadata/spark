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

package org.apache.spark.deploy.history

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.metrics.source.Source

/**
 * A [[Source]] exposing byte-valued store usage of the [[FsHistoryProvider]] through the metrics
 * system (e.g. the Prometheus servlet).
 *
 * The disk store gauges are only registered when a disk-based KVStore is configured
 * (`spark.history.store.path`), and the memory store gauges only when the hybrid store is enabled.
 * Sharing the "history" source name keeps all History Server metrics under one namespace.
 */
private[history] class FsHistoryProviderSource(
    diskManager: Option[HistoryServerDiskManager],
    memoryManager: Option[HistoryServerMemoryManager]) extends Source {

  override val sourceName: String = "history"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  private def registerGauge(name: String, value: => Long): Unit = {
    metricRegistry.register(name, new Gauge[Long] {
      override def getValue: Long = value
    })
  }

  diskManager.foreach { dm =>
    registerGauge(MetricRegistry.name("diskStore", "usedBytes"), dm.currentUsageBytes)
    registerGauge(MetricRegistry.name("diskStore", "committedBytes"), dm.committedUsageBytes)
    registerGauge(MetricRegistry.name("diskStore", "maxBytes"), dm.maxUsageBytes)
  }

  memoryManager.foreach { mm =>
    registerGauge(MetricRegistry.name("memoryStore", "usedBytes"), mm.currentUsageBytes)
    registerGauge(MetricRegistry.name("memoryStore", "maxBytes"), mm.maxUsageBytes)
  }
}
