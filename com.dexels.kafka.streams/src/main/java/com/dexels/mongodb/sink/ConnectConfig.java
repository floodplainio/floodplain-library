package com.dexels.mongodb.sink;

import java.util.Map;

import org.apache.kafka.connect.runtime.WorkerConfig;

public class ConnectConfig extends WorkerConfig {

	public ConnectConfig( Map<String, String> props) {
		super(WorkerConfig.baseConfigDef(), props);
	}

}
