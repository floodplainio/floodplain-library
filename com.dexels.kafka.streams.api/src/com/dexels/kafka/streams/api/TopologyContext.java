package com.dexels.kafka.streams.api;

import java.util.Optional;

public class TopologyContext {
	public final Optional<String> tenant;
	public final String deployment;
	public final String instance;
	public final String generation;
	
	public TopologyContext(Optional<String> tenant, String deployment, String instance, String generation) {
		this.tenant = tenant;
		this.deployment = deployment;
		this.instance = instance;
		this.generation = generation;
	}

	public TopologyContext withInstance(String newInstance) {
		return new TopologyContext(tenant, deployment, newInstance, generation);
	}
}
