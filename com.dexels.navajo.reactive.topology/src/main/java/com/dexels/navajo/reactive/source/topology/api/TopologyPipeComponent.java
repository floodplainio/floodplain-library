package com.dexels.navajo.reactive.source.topology.api;

import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.streams.Topology;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;

public interface TopologyPipeComponent {
	public int addToTopology(Stack<String> transformerNames, int currentPipeId,  Topology topology, TopologyContext topologyContext,TopologyConstructor topologyConstructor);

	
}
