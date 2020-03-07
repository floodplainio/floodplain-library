package com.dexels.mongodb.sink;

import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.Map;

public class ConnectConfig extends WorkerConfig {

	public ConnectConfig( Map<String, String> props) {
		super(WorkerConfig.baseConfigDef(), props);
	}

}
