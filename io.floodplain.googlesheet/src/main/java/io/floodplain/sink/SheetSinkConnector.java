package io.floodplain.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class SheetSinkConnector extends SinkConnector {

	private Map<String, String> props;
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return "0.1";
	}

	@Override
	public void start(Map<String, String> props) {
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
	}

	@Override
	public ConfigDef config() {
		return  new ConfigDef();
	}

}
