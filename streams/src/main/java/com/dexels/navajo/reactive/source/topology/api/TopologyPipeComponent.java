package com.dexels.navajo.reactive.source.topology.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import org.apache.kafka.streams.Topology;

import java.util.Stack;

public interface TopologyPipeComponent {
	public void addToTopology(Stack<String> transformerNames, int currentPipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage);
	public boolean materializeParent();
	public void setMaterialize();
	public boolean materialize();
	
}
