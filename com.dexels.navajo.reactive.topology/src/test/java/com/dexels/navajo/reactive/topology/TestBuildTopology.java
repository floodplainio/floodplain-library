package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Stack;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.Topology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.CoreReactiveFinder;
import com.dexels.navajo.reactive.ReactiveStandalone;
import com.dexels.navajo.reactive.api.CompiledReactiveScript;
import com.dexels.navajo.reactive.api.Reactive;
import com.dexels.navajo.reactive.api.ReactivePipe;
import com.dexels.navajo.reactive.source.topology.TopologyReactiveFinder;
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TestBuildTopology {
	
	AdminClient adminClient;
	private Map<String, MessageTransformer> transformerRegistry = Collections.emptyMap();
	@Before
	public void setup() {
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		Properties props = new Properties();
		props.put("bootstrap.servers","kafka:9092");
		adminClient = AdminClient.create(props);
		// TODO fill in props
//		Reactive.finderInstance().addReactiveSourceFactory(new MongoReactiveSourceFactory(), "topic");

	}
	
	@Test
	public void testSimpleTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("simpletopic.rr"));
		TopologyContext topologyContext = new TopologyContext(Optional.of("SOMETENANT"), "SOMEDEPLOYMENT", "someinstance", "1");
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test
	public void testJoinTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("jointopic.rr"));
		TopologyContext topologyContext = new TopologyContext(Optional.of("SOMETENANT"), "SOMEDEPLOYMENT", "someinstance", "1");
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);

		System.err.println("Topology: \n"+topology.describe());
	}
}
