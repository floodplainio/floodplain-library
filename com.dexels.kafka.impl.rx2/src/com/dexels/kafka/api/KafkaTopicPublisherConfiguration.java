package com.dexels.kafka.api;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
@ApplicationScoped
public class KafkaTopicPublisherConfiguration {
	public String bootstrapServers;
	public Optional<Integer> retries;
	public Short replicationFactor;
	public Integer partitions;
	public Optional<String> compression;
	
	
	public KafkaTopicPublisherConfiguration() {
		
	}
	public KafkaTopicPublisherConfiguration(String bootstrapServers, Optional<Integer> retries, short replicationFactor, int partitions,Optional<String> compression) {
		this.bootstrapServers = bootstrapServers;
		this.retries = retries;
		this.replicationFactor = replicationFactor;
		this.partitions = partitions;
		this.compression = compression;
	}
}
