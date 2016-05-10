/*
 * Copyright 2016 IBM Corp.
 * 
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

package org.apache.spark.elk.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit
import com.codahale.metrics.MetricRegistry
import com.ibm.spark.elk.metrics.reporter.ElasticsearchReporter
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.SecurityManager

class ElasticsearchSink(val properties: Properties, val registry: MetricRegistry,
    securityMgr: SecurityManager) extends Sink {

  // Property keys
  val ELASTICSEARCH_KEY_HOST = "host"
  val ELASTICSEARCH_KEY_PORT = "port"
  val ELASTICSEARCH_KEY_INDEX = "index"
  val ELASTICSEARCH_KEY_PERIOD = "period"
  val ELASTICSEARCH_KEY_UNIT = "unit"

  // Defaults
  val ELASTICSEARCH_DEFAULT_PERIOD = 10
  val ELASTICSEARCH_DEFAULT_UNIT = "SECONDS"
  val ELASTICSEARCH_DEFAULT_INDEX = "spark"

  // Host and port must be specified
  val host = properties.getProperty(ELASTICSEARCH_KEY_HOST)
  if (host == null) {
    throw new Exception("'host' property not specified for Elasticsearch sink.")
  }
  val port = properties.getProperty(ELASTICSEARCH_KEY_PORT)
  if (port == null) {
    throw new Exception("'port' property not specified for Elasticsearch sink")
  }

  val index = properties.getProperty(ELASTICSEARCH_KEY_INDEX, ELASTICSEARCH_DEFAULT_INDEX)

  val pollPeriod = Option(properties.getProperty(ELASTICSEARCH_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => ELASTICSEARCH_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(properties.getProperty(ELASTICSEARCH_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase())
    case None => TimeUnit.valueOf(ELASTICSEARCH_DEFAULT_UNIT)
  }

  val reporter: ElasticsearchReporter = ElasticsearchReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .host(host)
    .port(port)
    .index(index)
    .build();

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
