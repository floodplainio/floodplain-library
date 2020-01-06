package com.dexels.kafka.api;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
@ApplicationScoped
public class KafkaTopicPublisherConfiguration {
	
	Optional<Integer> retries;
	Short replicationFactor;
	Integer partitions;
	Optional<String> compression;
	
	@ConfigProperty(name = "io.floodplain.bootstrapServers",defaultValue = "blabla")
	String bootstrapServers;
	
	public Optional<Integer> retries() {
		return retries;
	}
	public Short replicationFactor() {
		return replicationFactor;
	}
	public Integer partitions() {
		return partitions;
	}
	public Optional<String> compression() {
		return compression;
	}


	
	public KafkaTopicPublisherConfiguration() {
		System.err.println("Created kafka topic pubconfig");
	}
	public KafkaTopicPublisherConfiguration(String bootstrapServers, Optional<Integer> retries, short replicationFactor, int partitions,Optional<String> compression) {
		this.bootstrapServers = bootstrapServers;
		this.retries = retries;
		this.replicationFactor = replicationFactor;
		this.partitions = partitions;
		this.compression = compression;
	}
	
	public String bootstrapServers() {
		return this.bootstrapServers;
	}
}
