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
 * A base [[Source]] exposing high level metrics of the [[HistoryServer]] so that they can be
 * scraped through the metrics system sinks (e.g. the Prometheus servlet).
 *
 * These are intentionally cheap, server-wide gauges. Per-application metrics are out of scope.
 */
private[history] class HistoryServerSource(server: HistoryServer) extends Source {

  override val sourceName: String = "history"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // Number of applications currently known to the history provider.
  metricRegistry.register(MetricRegistry.name("application", "count"), new Gauge[Int] {
    override def getValue: Int = server.getApplicationList().size
  })

  // Number of event logs still being replayed / pending processing.
  metricRegistry.register(MetricRegistry.name("eventLog", "underProcessCount"), new Gauge[Int] {
    override def getValue: Int = server.getEventLogsUnderProcess()
  })

  // Epoch millis of the last time the provider finished scanning the log directory.
  metricRegistry.register(MetricRegistry.name("lastUpdated"), new Gauge[Long] {
    override def getValue: Long = server.getLastUpdatedTime()
  })
}
