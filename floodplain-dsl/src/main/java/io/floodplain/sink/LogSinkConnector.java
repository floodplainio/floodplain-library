/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogSinkConnector extends SinkConnector {

	private Map<String, String> props;
	
	private final static Logger logger = LoggerFactory.getLogger(LogSinkConnector.class);

	@Override
	public String version() {
		return "0.10";
	}

	@Override
	public void start(Map<String, String> props) {
		logger.info("Log sink connector: {}",props);
    	this.props = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return LogSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		Map<String,String> taskConfig = new HashMap<>();
		taskConfig.putAll(props);
		return Arrays.asList(taskConfig);
	}

	@Override
	public void stop() {
		logger.info("Stopping google sheet sink");
	}

	@Override
	public ConfigDef config() {
		return  new ConfigDef();
	}

}
