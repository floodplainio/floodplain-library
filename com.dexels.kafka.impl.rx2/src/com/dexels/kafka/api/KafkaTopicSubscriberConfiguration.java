package com.dexels.kafka.api;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class KafkaTopicSubscriberConfiguration {
	
	@ConfigProperty(name="bootstrapServers")
	public String bootstrapHosts;

	public int maxWaitMillis;
	
	public int maxRecordCount;
	
	public KafkaTopicSubscriberConfiguration() {
		
	}
	public KafkaTopicSubscriberConfiguration(String bootstrapHosts, int maxWaitMillis, int maxRecordCount) {
		this.bootstrapHosts = bootstrapHosts;
		this.maxWaitMillis = maxWaitMillis;
		this.maxRecordCount = maxRecordCount;
	}
}
