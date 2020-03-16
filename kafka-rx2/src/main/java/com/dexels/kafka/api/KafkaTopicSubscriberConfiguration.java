package com.dexels.kafka.api;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class KafkaTopicSubscriberConfiguration {
	
	int maxWaitMillis;
	int maxRecordCount;
	
	@ConfigProperty(name="io.floodplain.bootstrapServers", defaultValue = "aapaap")
	String bootstrapHosts;

	public String bootstrapHosts() {
		return bootstrapHosts;
	}
	public int maxWaitMillis() {
		return maxWaitMillis;
	}
	public int maxRecordCount() {
		return maxRecordCount;
	}
	
	public KafkaTopicSubscriberConfiguration() {
		
	}
	public KafkaTopicSubscriberConfiguration(String bootstrapHosts, int maxWaitMillis, int maxRecordCount) {
		this.bootstrapHosts = bootstrapHosts;
		this.maxWaitMillis = maxWaitMillis;
		this.maxRecordCount = maxRecordCount;
	}
}
