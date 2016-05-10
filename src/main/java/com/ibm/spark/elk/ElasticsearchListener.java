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

package com.ibm.spark.elk;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.JavaSparkListener;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.scheduler.TaskInfo;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.JsonProtocol;
import org.json4s.JsonAST.JValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import scala.Option;

/**
 * A SparkListener to write Spark events to an Elasticsearch index
 *
 */
public class ElasticsearchListener extends JavaSparkListener {

	private final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchListener.class);

	private String esHost;
	private String esPort;
	private String esIndex;
	private String timestampField = "timestamp"; // TOFDO make configurable?

	private final String timeStampString = "YYYY-MM-dd'T'HH:mm:ss.SSSZ";
	private SimpleDateFormat timestampFormat = new SimpleDateFormat(timeStampString);

	private String appId = "";
	private String appName = "";

	private final String localhost = org.apache.spark.util.Utils.localHostName();
	private final String localExecutorID = "driver";

	private JsonFactory jsonFactory = new JsonFactory();
	private ElasticsearchConnector esConnector;
	HttpURLConnection conn = null;

	private boolean indexInitialized = false;

	private long bulkIntervalTimeout;

	private int eventCount;

	// Delay interval for bulk updates
	private long BULK_POST_INTERVAL_MS = 30000; // TODO make this configurable

	// Number of events for each bulk update
	private int BULK_POST_EVENT_COUNT = 1000; // TODO make this configurable

	/**
	 * Listener constructor.
	 * 
	 * Attempts to establish the default mappings in the Elasticsearch index.
	 * 
	 * @param conf
	 */
	public ElasticsearchListener(SparkConf conf) {
		esHost = conf.get("spark.elasticsearch.host", "localhost");
		esPort = conf.get("spark.elasticsearch.port", "9200");
		esIndex = conf.get("spark.elasticsearch.index", "spark");
		appId = conf.getAppId();
		appName = conf.get("spark.app.name", "");
		esConnector = new ElasticsearchConnector(esHost, esPort, esIndex);
		indexInitialized = esConnector.addDefaultMappings();
		if (!indexInitialized) {
			LOGGER.warn("Failed to initialize Elasticsearch index '" + esIndex + "' on " + esHost + ":" + esPort);
		}

	}

	@Override
	public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

		Long time = System.currentTimeMillis();
		Option<Object> endTime = stageCompleted.stageInfo().completionTime();
		if (endTime.isDefined()) {
			time = (Long) endTime.get();
		}

		writeEvent("stageComleted", time, localhost, localExecutorID, stageCompleted);
	}

	@Override
	public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
		Long time = System.currentTimeMillis();
		Option<Object> startTime = stageSubmitted.stageInfo().submissionTime();
		if (startTime.isDefined()) {
			time = (Long) startTime.get();
		}

		/*
		 * Elasticsearch 2 does not allow '.'s in field names so for now we will
		 * ignore the properties by creating a copy of the event
		 */
		SparkListenerStageSubmitted newStageSubmitted = stageSubmitted.copy(stageSubmitted.stageInfo(), null);
		writeEvent("stageSubmitted", time, localhost, localExecutorID, newStageSubmitted);
	}

	@Override
	public void onTaskStart(SparkListenerTaskStart taskStart) {
		final TaskInfo taskInfo = taskStart.taskInfo();

		writeEvent("taskStart", taskInfo.launchTime(), taskInfo.host(), taskInfo.executorId(), taskStart);
	}

	@Override
	public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
		final TaskInfo taskInfo = taskGettingResult.taskInfo();
		writeEvent("taskGettingResult", taskInfo.gettingResultTime(), taskInfo.host(), taskInfo.executorId(),
				taskGettingResult);
	}

	@Override
	public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
		final TaskInfo taskInfo = taskEnd.taskInfo();
		writeEvent("taskEnd", taskInfo.finishTime(), taskInfo.host(), taskInfo.executorId(), taskEnd);
	}

	@Override
	public void onJobStart(SparkListenerJobStart jobStart) {

		/*
		 * Elasticsearch 2 does not allow '.'s in field names so for now we will
		 * ignore the properties by creating a copy of the event
		 */
		SparkListenerJobStart newJobStart = jobStart.copy(jobStart.jobId(), jobStart.time(), jobStart.stageInfos(),
				null);
		writeEvent("jobStart", jobStart.time(), localhost, localExecutorID, newJobStart);
	}

	@Override
	public void onJobEnd(SparkListenerJobEnd jobEnd) {
		writeEvent("jobEnd", jobEnd.time(), localhost, localExecutorID, jobEnd);
	}

	@Override
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
	}

	@Override
	public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
		final BlockManagerId bmId = blockManagerAdded.blockManagerId();
		writeEvent("blockManagerAdded", blockManagerAdded.time(), bmId.host(), bmId.executorId(), blockManagerAdded);
	}

	@Override
	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
		final BlockManagerId bmId = blockManagerRemoved.blockManagerId();
		writeEvent("blockManagerRemoved", blockManagerRemoved.time(), bmId.host(), bmId.executorId(),
				blockManagerRemoved);
	}

	@Override
	public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
		appId = applicationStart.appId().get();
		appName = applicationStart.appName();

		writeEvent("applicationStart", applicationStart.time(), localhost, localExecutorID, applicationStart);
	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
		writeEvent("applicationEnd", applicationEnd.time(), localhost, localExecutorID, applicationEnd);
		
		// Force bulk update on application end
		sendBulk();
	}

	@Override
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
	}

	@Override
	public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
		final ExecutorInfo info = executorAdded.executorInfo();
		writeEvent("executorAdded", executorAdded.time(), info.executorHost(), executorAdded.executorId(),
				executorAdded);
	}

	@Override
	public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
		writeEvent("executorRemoved", executorRemoved.time(), "", executorRemoved.executorId(), executorRemoved);
	}

	@Override
	public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
	}

	/**
	 * Write a Spark event to the Elasticsearch index
	 * 
	 * The events are serialized to the connection output stream but only sent
	 * to the Elasticsearch server using bulk updates. The update will occur
	 * after BULK_POST_EVENT_COUNT events are processed or after the time
	 * BULK_POST_INTERVAL_MS has passed since opening the connection.
	 * 
	 * @param name
	 *            event name
	 * @param time
	 *            event timestamp
	 * @param host
	 *            host on which the event occurred
	 * @param executorId
	 *            executor on which the event occurred
	 * @param event
	 *            the event itself
	 */
	private void writeEvent(String name, long time, String host, String executorId, SparkListenerEvent event) {
		if (!indexInitialized) {
			return;
		}

		String timestampString = timestampFormat.format(new Date(time));

		try {
			if (conn == null) {
				conn = esConnector.getConnection("POST", "spark_event/_bulk");

				if (conn == null) {
					return;
				}

				bulkIntervalTimeout = System.currentTimeMillis() + BULK_POST_INTERVAL_MS;
				eventCount = 0;
			}

			JsonGenerator jsonGenerator = jsonFactory.createGenerator(conn.getOutputStream());

			// Write the Elasticsearch bulk header
			jsonGenerator.writeStartObject();
			jsonGenerator.writeObjectFieldStart("index");
			jsonGenerator.writeEndObject();
			jsonGenerator.writeEndObject();
			jsonGenerator.flush();
			((OutputStream) (jsonGenerator.getOutputTarget())).write("\n".getBytes());

			// write standard fields
			jsonGenerator.writeStartObject();
			jsonGenerator.writeStringField(timestampField, timestampString);
			jsonGenerator.writeStringField("applicationName", appName);
			jsonGenerator.writeStringField("applicationId", appId);
			jsonGenerator.writeStringField("hostName", host);
			jsonGenerator.writeStringField("executorId", executorId);
			jsonGenerator.writeFieldName(name);
			JValue v = JsonProtocol.sparkEventToJson(event);
			Utils.writeJValue(jsonGenerator, v);

			jsonGenerator.writeEndObject();

			jsonGenerator.flush();
			((OutputStream) (jsonGenerator.getOutputTarget())).write("\n".getBytes());

			if (eventCount++ > BULK_POST_EVENT_COUNT || System.currentTimeMillis() > bulkIntervalTimeout) {
				sendBulk();
			}
		} catch (IOException e) {
			LOGGER.error("Exception connecting to Elasticsearch", e);
		}

	}

	/**
	 * Sends the bulk update to Elasticsearch by closing the connection.
	 */
	private void sendBulk() {
		if (conn != null) {
			try {
				esConnector.endConnection(conn);
			} catch (IOException e) {
				LOGGER.error("Exception posting to Elasticsearch", e);
			}
			conn = null;
		}
	}

}
