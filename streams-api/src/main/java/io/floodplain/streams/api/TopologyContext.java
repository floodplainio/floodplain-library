package io.floodplain.streams.api;

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
        return tenant.orElse(DEFAULT_TENANT) + "-" + deployment + "-" + generation + "-" + instance;
    }

    public String qualifiedName(String name, int currentTransformer, int currentPipe) {
        return CoreOperators.topicName("@" + name + "_" + currentPipe + "_" + currentTransformer, this);
    }

//	private static String processorName(String sourceTopicName) {
//		return sourceTopicName.replace(':',  '_').replace('@', '.');
//	}


}
