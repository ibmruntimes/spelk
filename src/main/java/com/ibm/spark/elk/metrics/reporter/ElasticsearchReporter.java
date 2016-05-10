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

package com.ibm.spark.elk.metrics.reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.spark.elk.ElasticsearchConnector;

@SuppressWarnings("rawtypes")

public class ElasticsearchReporter extends ScheduledReporter {

	private final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReporter.class);

	private final Clock clock;
	private final String timestampField;

	private final String timeStampString = "YYYY-MM-dd'T'HH:mm:ss.SSSZ";
	private SimpleDateFormat timestampFormat;

	private String localhost;
	private String appName = null;
	private String appId = null;
	private String executorId = null;
	private boolean indexInitialized = false;

	private ElasticsearchConnector connector;
	private JsonFactory jsonFactory;

	private ElasticsearchReporter(MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit,
			TimeUnit durationUnit, String host, String port, String indexName, String timestampField) {

		super(registry, "elasticsearch-reporter", filter, rateUnit, durationUnit);

		this.clock = Clock.defaultClock();
		this.connector = new ElasticsearchConnector(host, port, indexName);
		this.timestampField = timestampField;
		this.timestampFormat = new SimpleDateFormat(timeStampString);
		this.localhost = Utils.localHostName();

		jsonFactory = new JsonFactory();

		indexInitialized = connector.addDefaultMappings();
		if (!indexInitialized) {
			LOGGER.warn("Failed to initialize Elasticsearch index '" + indexName + "' on " + host + ":" + port);
		}
	}

	@Override
	public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
			SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

		if (!indexInitialized) {
			return;
		}

		final long timestamp = clock.getTime();
		String timestampString = timestampFormat.format(new Date(timestamp));

		if (appName == null) {
			SparkConf conf = SparkEnv.get().conf();
			appName = conf.get("spark.app.name", "");
			appId = conf.getAppId();
			executorId = conf.get("spark.executor,id", null);
		}

		try {

			HttpURLConnection connection = connector.getConnection("POST", "/_bulk");
			if (connection == null) {
				return;
			}

			JsonGenerator jsonGenerator = jsonFactory.createGenerator(connection.getOutputStream());
			ObjectMapper objectMapper = new ObjectMapper();
			jsonGenerator.setCodec(objectMapper);

			if (!gauges.isEmpty()) {
				for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
					reportGauge(jsonGenerator, entry, timestampString);
				}
			}

			if (!counters.isEmpty()) {
				for (Map.Entry<String, Counter> entry : counters.entrySet()) {
					reportCounter(jsonGenerator, entry, timestampString);
				}
			}

			if (!histograms.isEmpty()) {
				for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
					reportHistogram(jsonGenerator, entry, timestampString);
				}
			}

			if (!meters.isEmpty()) {
				for (Map.Entry<String, Meter> entry : meters.entrySet()) {
					reportMeter(jsonGenerator, entry, timestampString);
				}
			}

			if (!timers.isEmpty()) {
				for (Map.Entry<String, Timer> entry : timers.entrySet()) {
					reportTimer(jsonGenerator, entry, timestampString);
				}
			}

			if (connector.endConnection(connection) != HttpURLConnection.HTTP_OK) {
				LOGGER.warn("Failed to write metric to Elasticsearch");
			}

		} catch (IOException ioe) {
			LOGGER.error("Exception posting to Elasticsearch index: " + ioe.toString());
		}

	}

	private void reportGauge(JsonGenerator jsonGenerator, Map.Entry<String, Gauge> entry, String timestampString) {
		try {

			writeStartMetric(entry.getKey(), jsonGenerator, timestampString);
			jsonGenerator.writeObject((entry.getValue().getValue()));
			writeEndMetric(jsonGenerator);

		} catch (IOException ioe) {
			LOGGER.error("Exception writing metrics to Elasticsearch index: " + ioe.toString());
		}

	}

	private void reportCounter(JsonGenerator jsonGenerator, Entry<String, Counter> entry, String timestampString) {
		try {

			writeStartMetric(entry.getKey(), jsonGenerator, timestampString);
			jsonGenerator.writeNumber((entry.getValue().getCount()));
			writeEndMetric(jsonGenerator);

		} catch (IOException ioe) {
			LOGGER.error("Exception writing metrics to Elasticsearch index: " + ioe.toString());
		}

	}

	private void reportHistogram(JsonGenerator jsonGenerator, Entry<String, Histogram> entry, String timestampString) {
		try {
			writeStartMetric(entry.getKey(), jsonGenerator, timestampString);
			jsonGenerator.writeStartObject();
			final Histogram histogram = entry.getValue();
			final Snapshot snapshot = histogram.getSnapshot();
			jsonGenerator.writeNumberField("count", histogram.getCount());
			jsonGenerator.writeNumberField("min", convertDuration(snapshot.getMin()));
			jsonGenerator.writeNumberField("max", convertDuration(snapshot.getMax()));
			jsonGenerator.writeNumberField("mean", convertDuration(snapshot.getMean()));
			jsonGenerator.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
			jsonGenerator.writeNumberField("median", convertDuration(snapshot.getMedian()));
			jsonGenerator.writeNumberField("75th percentile", convertDuration(snapshot.get75thPercentile()));
			jsonGenerator.writeNumberField("95th percentile", convertDuration(snapshot.get95thPercentile()));
			jsonGenerator.writeNumberField("98th percentile", convertDuration(snapshot.get98thPercentile()));
			jsonGenerator.writeNumberField("99th percentile", convertDuration(snapshot.get99thPercentile()));
			jsonGenerator.writeNumberField("999th percentile", convertDuration(snapshot.get999thPercentile()));

			jsonGenerator.writeEndObject();
			writeEndMetric(jsonGenerator);

		} catch (IOException ioe) {
			LOGGER.error("Exception writing metrics to Elasticsearch index: " + ioe.toString());
		}

	}

	private void reportMeter(JsonGenerator jsonGenerator, Entry<String, Meter> entry, String timestampString) {
		try {
			writeStartMetric(entry.getKey(), jsonGenerator, timestampString);
			jsonGenerator.writeStartObject();
			final Meter meter = entry.getValue();
			jsonGenerator.writeNumberField("count", meter.getCount());
			jsonGenerator.writeNumberField("mean rate", convertRate(meter.getMeanRate()));
			jsonGenerator.writeNumberField("1-minute rate", convertRate(meter.getOneMinuteRate()));
			jsonGenerator.writeNumberField("5-minute rate", convertRate(meter.getFiveMinuteRate()));
			jsonGenerator.writeNumberField("15-minute rate", convertRate(meter.getFifteenMinuteRate()));
			jsonGenerator.writeEndObject();
			writeEndMetric(jsonGenerator);

		} catch (IOException ioe) {
			LOGGER.error("Exception writing metrics to Elasticsearch index: " + ioe.toString());
		}

	}

	private void reportTimer(JsonGenerator jsonGenerator, Entry<String, Timer> entry, String timestampString) {
		try {
			writeStartMetric(entry.getKey(), jsonGenerator, timestampString);
			jsonGenerator.writeStartObject();
			final Timer timer = entry.getValue();
			final Snapshot snapshot = timer.getSnapshot();
			jsonGenerator.writeNumberField("count", timer.getCount());
			jsonGenerator.writeNumberField("mean rate", convertRate(timer.getMeanRate()));
			jsonGenerator.writeNumberField("1-minute rate", convertRate(timer.getOneMinuteRate()));
			jsonGenerator.writeNumberField("5-minute rate", convertRate(timer.getFiveMinuteRate()));
			jsonGenerator.writeNumberField("15-minute rate", convertRate(timer.getFifteenMinuteRate()));
			jsonGenerator.writeNumberField("min", convertDuration(snapshot.getMin()));
			jsonGenerator.writeNumberField("max", convertDuration(snapshot.getMax()));
			jsonGenerator.writeNumberField("mean", convertDuration(snapshot.getMean()));
			jsonGenerator.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
			jsonGenerator.writeNumberField("median", convertDuration(snapshot.getMedian()));
			jsonGenerator.writeNumberField("75th percentile", convertDuration(snapshot.get75thPercentile()));
			jsonGenerator.writeNumberField("95th percentile", convertDuration(snapshot.get95thPercentile()));
			jsonGenerator.writeNumberField("98th percentile", convertDuration(snapshot.get98thPercentile()));
			jsonGenerator.writeNumberField("99th percentile", convertDuration(snapshot.get99thPercentile()));
			jsonGenerator.writeNumberField("999th percentile", convertDuration(snapshot.get999thPercentile()));

			jsonGenerator.writeEndObject();
			writeEndMetric(jsonGenerator);

		} catch (IOException ioe) {
			LOGGER.error("Exception writing metrics to Elasticsearch index: " + ioe.toString());
		}

	}

	private String writeStartMetric(String name, JsonGenerator jsonGenerator, String timestampString)
			throws IOException {

		// Parse name to extract application id etc
		// The appid and executorId should be the same for all metrics so we can
		// process just the first one
		if (appId == null || executorId == null) {
			String nameParts[] = name.split("\\.");
			appId = nameParts[0];
			executorId = nameParts[1];
		}

		final String metricName = (name.substring(appId.length() + executorId.length() + 2)).replace('.', '_');

		// Write the Elasticsearch bulk header
		jsonGenerator.writeStartObject();
		jsonGenerator.writeObjectFieldStart("index");
		jsonGenerator.writeStringField("_type", metricName);
		jsonGenerator.writeEndObject();
		jsonGenerator.writeEndObject();
		jsonGenerator.flush();
		((OutputStream) (jsonGenerator.getOutputTarget())).write("\n".getBytes());

		// write standard fields
		jsonGenerator.writeStartObject();
		jsonGenerator.writeStringField(timestampField, timestampString);
		jsonGenerator.writeStringField("hostName", localhost);
		jsonGenerator.writeStringField("applicationName", appName);
		jsonGenerator.writeStringField("applicationId", appId);
		jsonGenerator.writeStringField("executorId", executorId);
		jsonGenerator.writeFieldName(metricName);

		return metricName;
	}

	private void writeEndMetric(JsonGenerator jsonGenerator) throws IOException {

		jsonGenerator.writeEndObject();
		jsonGenerator.flush();
		((OutputStream) (jsonGenerator.getOutputTarget())).write("\n".getBytes());
	}

	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}

	public static class Builder {
		private final MetricRegistry registry;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;

		private String elasticsearchHost = "localhost";
		private String elasticsearchPort = "9200";
		private String elasticsearchIndex = "spark";
		private String elasticsearchTimestampField = "timestamp";

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		public Builder host(String hostName) {
			this.elasticsearchHost = hostName;
			return this;
		}

		public Builder port(String port) {
			this.elasticsearchPort = port;
			return this;
		}

		public Builder index(String index) {
			this.elasticsearchIndex = index;
			return this;
		}

		public Builder timestampField(String timestampFieldName) {
			this.elasticsearchTimestampField = timestampFieldName;
			return this;
		}

		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		public ElasticsearchReporter build() {
			return new ElasticsearchReporter(registry, filter, rateUnit, durationUnit, elasticsearchHost,
					elasticsearchPort, elasticsearchIndex, elasticsearchTimestampField);
		}
	}

}
