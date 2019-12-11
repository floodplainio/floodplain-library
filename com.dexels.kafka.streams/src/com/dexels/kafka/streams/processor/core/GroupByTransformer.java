package com.dexels.kafka.streams.processor.core;

import java.util.Optional;
import java.util.Stack;

import org.apache.kafka.streams.Topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.processor.api.TopologyTransformer;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;

public class GroupByTransformer implements TopologyTransformer {

	private final boolean ignoreOriginalKey;
	private final String key;
	private String name;
	private Optional<String> from;
	public GroupByTransformer(String key, String name, Optional<String> from,boolean ignoreOriginalKey) {
		this.key = key;
		this.name = name;
		this.from = from;
		this.ignoreOriginalKey = ignoreOriginalKey;
	}

	@Override
	public void addTransformerToTopology(Topology topology,TopologyContext topologyContext, String instanceName,
			TopologyConstructor topologyConstructor, int pipeNumber, int transformerNumber) {
		ReplicationTopologyParser.addGroupedProcessor(topology, topologyContext, topologyConstructor, name, from, ignoreOriginalKey, key, ()->null);
	}

}
