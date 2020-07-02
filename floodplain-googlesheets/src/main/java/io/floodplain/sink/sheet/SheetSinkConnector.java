package io.floodplain.sink.sheet;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SheetSinkConnector extends SinkConnector {

	private Map<String, String> props;
	
	private final static Logger logger = LoggerFactory.getLogger(SheetSinkConnector.class);

	@Override
	public String version() {
		return "0.10";
	}

	@Override
	public void start(Map<String, String> props) {
		logger.info("Sheet connector: {}",props);
    	this.props = props;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return SheetSinkTask.class;
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
