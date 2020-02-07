package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.CoreReactiveFinder;
import com.dexels.navajo.reactive.api.Reactive;
import com.dexels.navajo.reactive.source.topology.TopologyReactiveFinder;
import com.dexels.navajo.reactive.source.topology.TopologyRunner;

public class TestBuildTopology {
	
	
	AdminClient adminClient;
	private TopologyContext topologyContext;
//	private Properties props;

	private String brokers = "kafka:9092";
	private String storagePath = "mystorage";

//	private TopologyConstructor topologyConstructor;
	
	public TopologyRunner runner = null;
	@Before
	public void setup() {
		String applicationId = UUID.randomUUID().toString();
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		topologyContext = new TopologyContext(Optional.of("Generic"), "test", "someinstance", "20200207");
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		runner = new TopologyRunner(topologyContext,brokers,storagePath,applicationId);
	}
	
	private void runTopology(Topology topology, Optional<StreamConfiguration> streamConfiguration) throws InterruptedException, IOException {
		KafkaStreams stream = runner.runTopology(topology, streamConfiguration);
		for (int i = 0; i < 50; i++) {
			boolean isRunning = stream.state().isRunning();
	        String stateName = stream.state().name();
	        System.err.println("State: "+stateName+" - "+isRunning);
	        final Collection<StreamsMetadata> allMetadata = stream.allMetadata();
	        System.err.println("meta: "+allMetadata);
			Thread.sleep(200);
		}
		stream.close();
		Thread.sleep(1000);
	}
	
	@Test
	public void testSimpleTopic() throws ParseException, IOException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("simpletopic.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test
	public void testDatabase() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("database.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology,Optional.empty());
	}
	
	@Test
	public void testStorelessTopic() throws ParseException, IOException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("simplewithoutstore.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
	}
	
	
	@Test
	public void testParseFolder() throws ParseException, IOException {
		Topology topology = runner.parseReactivePipeTopology(Paths.get("/Users/frank/git/dvdstore.replication").toFile(),Paths.get(this.storagePath));
	}
	
	@Test 
	public void testJoinTopic() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("jointopic.rr"),"junit");
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology,Optional.of(sc));
	}

	@Test
	public void testConfigurationStreamInstance() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("address.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test 
	public void testRemoteJoin() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("remotejoin.rr"),"junit");
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));
		System.err.println("Topology: \n"+topology.describe());

		runTopology(topology,Optional.of(sc));
	}

	@Test @Ignore
	public void testDebezium() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("sinkLog.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology,Optional.empty());
	}


	
	@Test @Ignore
	public void testAddressTopic() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("address.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology,Optional.empty());

	}

}
