package com.dexels.navajo.reactive.api;

import java.util.Optional;

public class TopologyContext {
	
	public static final String DEFAULT_TENANT = "DEFAULT";
	public final Optional<String> tenant;
	public final String deployment;
	public final String instance;
	public final String generation;
//	public final String brokers;
	
	
	public TopologyContext(Optional<String> tenant, String deployment, String instance, String generation) {
		this.tenant = tenant;
		this.deployment = deployment;
		this.instance = instance;
		this.generation = generation;
//		this.brokers = brokers;
	}

	public TopologyContext withInstance(String newInstance) {
		return new TopologyContext(tenant, deployment, newInstance, generation);
	}
	
	public String applicationId() {
		return tenant.orElse(DEFAULT_TENANT)+"-"+deployment+"-"+generation+"-"+instance;
	}

	private String processorName(String sourceTopicName) {
		return sourceTopicName.replace(':',  '_').replace('@', '.');
	}

//	public String qualifiedName(int pipeNr, int transformerNr, ParameterValidator validator) {
//
//	}




//	final String sourceProcessorName = processorName(topologyContext.instance+"_"+name+"_debezium_conversion_source")+"-"+topicName;
//   StreamScriptContext context =new StreamScriptContext(topologyContext.tenant.orElse(TopologyContext.DEFAULT_TENANT), topologyContext.instance, topologyContext.deployment);

}
