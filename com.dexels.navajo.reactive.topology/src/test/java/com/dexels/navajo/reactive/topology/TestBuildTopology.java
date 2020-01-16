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
		Topology topology = new Topology();
		Assert.assertEquals(1, crs.pipes.size());
		int pipeNr = 0;
		for (ReactivePipe pipe : crs.pipes) {
			processPipe(topologyContext, topologyConstructor, topology, pipeNr, pipe);
		}
		ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
		System.err.println("Topology: \n"+topology.describe());
	}

	private void processPipe(TopologyContext topologyContext, TopologyConstructor topologyConstructor, Topology topology,
			int pipeNr, ReactivePipe pipe) {
//		String pipeId = "pipe_"+pipeNr++;
//		AtomicInteger pipeNr = new AtomicInteger(initialPipeNr); 
		System.err.println("source name: "+pipe.source.getClass().getName());
		Stack<String> pipeStack = new Stack<>();
//			pipe.source.execute(context, current, paramMessage)
		TopologyPipeComponent sourceTopologyComponent = (TopologyPipeComponent)pipe.source;
		pipeNr = sourceTopologyComponent.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
		for (Object e : pipe.transformers) {
			System.err.println("Transformer: "+e);
			if(e instanceof TopologyPipeComponent) {
				TopologyPipeComponent tpc = (TopologyPipeComponent)e;
				tpc.addToTopology(pipeStack, pipeNr, topology, topologyContext, topologyConstructor);
			}
		}
//		pipe.transformers.forEach(e->{
//			if(e instanceof ReactiveTransformer) {
//				ReactiveTransformer rt = (ReactiveTransformer)e;
//				System.err.println("type: "+rt.metadata().name());
//				if(!rt.parameters().named.isEmpty()) {
//					System.err.println("named params:");
//					rt.parameters().named.entrySet().forEach(entry->{
//						System.err.println("param: "+entry.getKey()+" value: "+entry.getValue()+" type: "+entry.getValue().returnType());
//					});
//					System.err.println("|< end of named");
//					
//				}
//				if(!rt.parameters().unnamed.isEmpty()) {
//					rt.parameters().unnamed.forEach(elt->{
//						System.err.println("E: "+elt+" type: "+elt.returnType());
//					});
//				}
//			}
//		});
		System.err.println("pipe: "+pipe);
	}
	
	@Test
	public void testJoinTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("jointopic.rr"));
		TopologyContext topologyContext = new TopologyContext(Optional.of("SOMETENANT"), "SOMEDEPLOYMENT", "someinstance", "1");
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		Topology topology = new Topology();
		Assert.assertEquals(1, crs.pipes.size());
//		int pipeCount = 0;
		int pipeId = 0;
		for (ReactivePipe pipe : crs.pipes) {
			System.err.println("source name: "+pipe.source.getClass().getName());
			Stack<String> pipeStack = new Stack<>();
//			pipe.source.execute(context, current, paramMessage)
			TopologyPipeComponent sourceTopologyComponent = (TopologyPipeComponent)pipe.source;
			pipeId = sourceTopologyComponent.addToTopology(pipeStack, pipeId, topology, topologyContext, topologyConstructor);
			for (Object e : pipe.transformers) {
				System.err.println("Transformer: "+e);
				if(e instanceof TopologyPipeComponent) {
					TopologyPipeComponent tpc = (TopologyPipeComponent)e;
					pipeId = tpc.addToTopology(pipeStack, pipeId, topology, topologyContext, topologyConstructor);
				}
			}
//			pipe.transformers.forEach(e->{
//				System.err.println("Transformer: "+e);
//				if(e instanceof TopologyPipeComponent) {
//					TopologyPipeComponent tpc = (TopologyPipeComponent)e;
//					tpc.addToTopology(pipeStack, pipeId, topology, topologyContext, topologyConstructor);
//				}
//				if(e instanceof ReactiveTransformer) {
//					ReactiveTransformer rt = (ReactiveTransformer)e;
//					System.err.println("type: "+rt.metadata().name());
//					if(!rt.parameters().named.isEmpty()) {
//						System.err.println("named params:");
//						rt.parameters().named.entrySet().forEach(entry->{
//							System.err.println("param: "+entry.getKey()+" value: "+entry.getValue()+" type: "+entry.getValue().returnType());
//						});
//						System.err.println("|< end of named");
//						
//					}
//					if(!rt.parameters().unnamed.isEmpty()) {
//						rt.parameters().unnamed.forEach(elt->{
//							System.err.println("E: "+elt+" type: "+elt.returnType());
//						});
//					}
//				}
//			});
			System.err.println("pipe: "+pipe);
		}
		ReplicationTopologyParser.materializeStateStores(topologyConstructor, topology);
		System.err.println("PipeCount: "+pipeId);
		System.err.println("Topology: \n"+topology.describe());
	}
}
