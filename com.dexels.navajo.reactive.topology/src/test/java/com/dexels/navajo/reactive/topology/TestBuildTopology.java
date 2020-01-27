package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.CoreReactiveFinder;
import com.dexels.navajo.reactive.ReactiveStandalone;
import com.dexels.navajo.reactive.api.CompiledReactiveScript;
import com.dexels.navajo.reactive.api.Reactive;
import com.dexels.navajo.reactive.source.topology.TopologyReactiveFinder;
import com.dexels.replication.transformer.api.MessageTransformer;

public class TestBuildTopology {
	
	
	private final static Logger logger = LoggerFactory.getLogger(TestBuildTopology.class);

	AdminClient adminClient;
	private Map<String, MessageTransformer> transformerRegistry = Collections.emptyMap();
	private TopologyContext topologyContext;
	private Properties props;

	private String brokers = "kafka:9092";
	private String storagePath = "mystorage";

	private TopologyConstructor topologyConstructor;
	@Before
	public void setup() {
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		topologyContext = new TopologyContext(Optional.of("Generic"), "test", "someinstance", "3");
		props = StreamInstance.createProperties(UUID.randomUUID().toString(), brokers, storagePath);
		adminClient = AdminClient.create(props);
		topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.of(adminClient));
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		// TODO fill in props
//		Reactive.finderInstance().addReactiveSourceFactory(new MongoReactiveSourceFactory(), "topic");

	}
	
	@Test
	public void testSimpleTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("simpletopic.rr"));
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test
	public void testDatabase() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("database.rr"));
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		System.err.println("Topology: \n"+topology.describe());
	}
	
	@Test
	public void testStorelessTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("simplewithoutstore.rr"));
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		System.err.println("Topology: \n"+topology.describe());
	}
	
	@Test
	public void testJoinTopic() throws ParseException, IOException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("jointopic.rr"));
		TopologyConstructor topologyConstructor = new TopologyConstructor(transformerRegistry , Optional.empty());
		
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test
	public void testConfigurationStreamInstance() throws ParseException, IOException, InterruptedException {
		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("address.rr"));
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));
	}

	@Test
	public void testDebezium() throws ParseException, IOException, InterruptedException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("database.rr"));
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
//		public static Properties createProperties(String applicationId,String brokers, String storagePath) {
			
		System.err.println("Topology: \n"+topology.describe());
		KafkaStreams stream = new KafkaStreams(topology, props);
		stream.setUncaughtExceptionHandler((thread,exception)->{
			logger.error("Error in streams: ",exception);
		});
		stream.setStateListener((oldState,newState)->{
			logger.info("State moving from {} to {}",oldState,newState);
		});
		stream.start();
		for (int i = 0; i < 100; i++) {
			boolean isRunning = stream.state().isRunning();
	        String stateName = stream.state().name();
	        System.err.println("State: "+stateName+" - "+isRunning);
	        final Collection<StreamsMetadata> allMetadata = stream.allMetadata();
	        System.err.println("meta: "+allMetadata);
			Thread.sleep(5000);
		}

		stream.close();
		Thread.sleep(10000);
	}

	
	@Test @Ignore
	public void testAddressTopic() throws ParseException, IOException, InterruptedException {

		CompiledReactiveScript crs = ReactiveStandalone.compileReactiveScript(getClass().getClassLoader().getResourceAsStream("address.rr"));
		Topology topology = ReactivePipeParser.parseReactiveStreamDefinition(crs, topologyContext, topologyConstructor);
//		public static Properties createProperties(String applicationId,String brokers, String storagePath) {
			
		System.err.println("Topology: \n"+topology.describe());
		KafkaStreams stream = new KafkaStreams(topology, props);
		stream.setUncaughtExceptionHandler((thread,exception)->{
			logger.error("Error in streams: ",exception);
		});
		stream.setStateListener((oldState,newState)->{
			logger.info("State moving from {} to {}",oldState,newState);
		});
		stream.start();
		for (int i = 0; i < 100; i++) {
			boolean isRunning = stream.state().isRunning();
	        String stateName = stream.state().name();
//	        stream.state().§
	        System.err.println("State: "+stateName+" - "+isRunning);
	        final Collection<StreamsMetadata> allMetadata = stream.allMetadata();
	        System.err.println("meta: "+allMetadata);
			Thread.sleep(5000);
		}

		stream.close();
		Thread.sleep(10000);
	}

	public static void main(String[] args) throws ParseException, IOException, InterruptedException {
		TestBuildTopology tb = new TestBuildTopology();
		tb.setup();
		tb.testJoinTopic();
	}
}
