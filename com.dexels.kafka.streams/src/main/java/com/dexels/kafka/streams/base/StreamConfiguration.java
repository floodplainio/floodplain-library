package com.dexels.kafka.streams.base;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.dexels.kafka.streams.api.sink.SinkConfiguration;


public class StreamConfiguration {

	private final String kafkaHosts;
	private final String deployment;
	private final Map<String,SinkConfiguration> sinks;
	private AdminClient adminClient;
	private final int maxWait;
	private final int maxSize;
	private final int replicationFactor;
	private final Map<String,Object> config;
	
	public StreamConfiguration(String kafkaHosts, Map<String,SinkConfiguration> sinks, String deployment, int maxWait, int maxSize, int replicationFactor) {
		this.kafkaHosts = kafkaHosts;
		this.sinks = Collections.unmodifiableMap(new HashMap<>(sinks));
		this.deployment = deployment;
		this.config = new HashMap<>();
		this.config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaHosts);
		this.config.put(AdminClientConfig.CLIENT_ID_CONFIG ,UUID.randomUUID().toString());
		
		this.maxWait = maxWait;
		this.maxSize = maxSize;
		this.replicationFactor = replicationFactor;
	}
	
	public AdminClient adminClient() {
		if(this.adminClient==null) {
			this.adminClient = AdminClient.create(config);
		}
		return this.adminClient;
	}

	public String kafkaHosts() {
		return kafkaHosts;
	}
	
	public int kafkaSubscribeMaxWait() {
		return maxWait;
	}

	public int kafkaSubscribeMaxSize() {
		return maxSize;
	}

	public int kafkaReplicationFactor() {
		return replicationFactor;
	}

	public Map<String,SinkConfiguration> sinks() {
		return sinks;
	}
	
	public Optional<SinkConfiguration> sink(String name) {
		return Optional.ofNullable(this.sinks.get(name));
	}
	
	public String deployment() {
		return deployment;
	}
	
	public List<String> tenants() {
		Optional<String> tenantDefinition = Optional.ofNullable(System.getenv("TENANT_MASTER"));
		if(tenantDefinition.isPresent()) {
			return Arrays.asList(tenantDefinition.get().split(","));
		}
		return Collections.emptyList();
	}
}
