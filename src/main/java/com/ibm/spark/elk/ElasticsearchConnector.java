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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchConnector {

	private final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConnector.class);

	private final String connectionUrl;

	public ElasticsearchConnector(String host, String port, String index) {
		connectionUrl = "http://" + host + ":" + port + "/" + index + "/";
	}

	public HttpURLConnection getConnection(String method) throws IOException {
		return getConnection(method, "");
	}

	public HttpURLConnection getConnection(String method, String endpoint) throws IOException {
		URL url = new URL(connectionUrl + endpoint);
		return connect(url, method);
	}

	private HttpURLConnection connect(URL url, String method) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setConnectTimeout(1000);
		connection.setRequestMethod(method);
		connection.setDoOutput(true);
		connection.connect();
		return connection;
	}

	public int endConnection(HttpURLConnection connection) throws IOException {
		connection.getOutputStream().close();
		connection.disconnect();
		return connection.getResponseCode();
	}
	

	public synchronized boolean addDefaultMappings() {
		try {
			// Check if default mapping is present
			HttpURLConnection connection = getConnection("HEAD", "/_default_");
			if (connection != null) {
				if (HttpURLConnection.HTTP_OK == endConnection(connection)) {
					return true;
				}

				// Add our default mappings
				// TODO the index name is hard coded in the mappings file so maybe we should use
				// JsonGenerator here as the mappings file is now quite simple
				HttpURLConnection mappingsConnection = getConnection("PUT");
				InputStream is = ElasticsearchConnector.class
						.getResourceAsStream("mappings/ElasticsearchDefaultMappings.json");
				if (mappingsConnection != null && is != null) {
					byte[] buffer = new byte[1024];
					int bytesRead;
					while ((bytesRead = is.read(buffer)) != -1) {
						mappingsConnection.getOutputStream().write(buffer, 0, bytesRead);
					}
					if (HttpURLConnection.HTTP_OK == endConnection(mappingsConnection)) {
						return true;
					}

				}
			}

		} catch (IOException ioe) {
			LOGGER.error("Exception initializing Elasticsearch index: " + ioe.toString());
		}
		return false;

	}
}
