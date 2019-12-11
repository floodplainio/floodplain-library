package com.dexels.kafka.streams.api.sink;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SinkConfiguration {
	
	private final String type;
	private final String name;
	private final Map<String,String> settings;
	public SinkConfiguration(String type, String name, Map<String,String> settings) {
		this.type = type;
		this.name = name;
		this.settings = Collections.unmodifiableMap(new HashMap<>(settings));
	}
	
	public String type() {
		return type;
	}

	public String name() {
		return name;
	}

	public Map<String,String> settings() {
		return settings;
	}
}
