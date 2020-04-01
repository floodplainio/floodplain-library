package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import org.apache.kafka.streams.Topology;

import java.util.Optional;
import java.util.Stack;

public class TopicSource implements TopologyPipeComponent {

	private final String topicName;

	private boolean materialize = false;

	public TopicSource(String topicName) {
		this.topicName = topicName;
	}

	@Override
	public void addToTopology(Stack<String> transformerNames, int pipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor) {
		String source = ReplicationTopologyParser.addSourceStore(topology, topologyContext, topologyConstructor, Optional.empty(), topicName, Optional.empty(),this.materialize());
		KafkaUtils.ensureExistsSync(topologyConstructor.adminClient, source, Optional.empty());
		transformerNames.push(source);
	}

	@Override
	public boolean materializeParent() {
		return false;
	}
	@Override
	public void setMaterialize() {
		this.materialize   = true;
	}
	
	@Override
	public boolean materialize() {
		return this.materialize;
	}

}
