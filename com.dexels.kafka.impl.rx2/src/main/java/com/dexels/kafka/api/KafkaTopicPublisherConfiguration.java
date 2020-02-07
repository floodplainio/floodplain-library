package com.dexels.kafka.api;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
@ApplicationScoped
public class KafkaTopicPublisherConfiguration {
	
	@ConfigProperty(name="io.floodplain.retries")
	Optional<Integer> retries;
	@ConfigProperty(name="io.floodplain.replicationFactor")
	Short replicationFactor;
	@ConfigProperty(name="io.floodplain.partitions",defaultValue = "1")
	Integer partitions;
	
	@ConfigProperty(name="io.floodplain.compression")
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
		if(partitions==null) {
			return Optional.empty();
		}
		return compression;
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
