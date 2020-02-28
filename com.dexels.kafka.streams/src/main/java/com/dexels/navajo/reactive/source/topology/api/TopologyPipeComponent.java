package com.dexels.navajo.reactive.source.topology.api;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.Topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;

public interface TopologyPipeComponent {
	public void addToTopology(String namespace, Stack<String> transformerNames, int currentPipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor, ImmutableMessage stateMessage);
	public boolean materializeParent();
	public void setMaterialize();
	public boolean materialize();
	
}
