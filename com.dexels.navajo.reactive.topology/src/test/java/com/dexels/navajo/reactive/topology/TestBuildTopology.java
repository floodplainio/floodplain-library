package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;

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
import com.dexels.kafka.streams.remotejoin.ReplicationTopologyParser;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.CoreReactiveFinder;
import com.dexels.navajo.reactive.api.Reactive;
import com.dexels.navajo.reactive.source.topology.TopologyReactiveFinder;
import com.dexels.navajo.reactive.source.topology.TopologyRunner;

public class TestBuildTopology {
	
	
	AdminClient adminClient;
	private TopologyContext topologyContext;
//	private Properties props;

	private String storagePath = "mystorage";

//	private TopologyConstructor topologyConstructor;
	
	public TopologyRunner runner = null;
	@Before
	public void setup() {
		String applicationId = "junit_8"; // UUID.randomUUID().toString();
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		topologyContext = new TopologyContext(Optional.of("Generic"), "test", "someinstance", "20200217g");
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));
		runner = new TopologyRunner(topologyContext,storagePath,applicationId,sc,false);
	}
	
	private void runTopology(Topology topology) throws InterruptedException, IOException {
		KafkaStreams stream = runner.runTopology(topology);
		Runtime.getRuntime().addShutdownHook(new Thread() 
	    { 
	      public void run() 
	      { 
	  		stream.close();
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//			}
	      } 
	    }); 
		for (int i = 0; i < 500; i++) {
			boolean isRunning = stream.state().isRunning();
	        String stateName = stream.state().name();
	        System.err.println("State: "+stateName+" - "+isRunning);
	        final Collection<StreamsMetadata> allMetadata = stream.allMetadata();
	        System.err.println("meta: "+allMetadata);
			Thread.sleep(10000);
		}
	}
	


	@Test @Ignore
	public void testDatabase() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("database.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testStorelessTopic() throws ParseException, IOException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("simplewithoutstore.rr"),"junit");
		System.err.println("Topology: \n"+topology.describe());
	}
	
	
	// TODO fix, but I'd need to fix all the files, I don't know which one is acting up.
	@Test @Ignore
	public void testParseFolder() throws ParseException, IOException {
		Topology topology = runner.parseReactivePipeTopology(Paths.get("/Users/frank/git/dvdstore.replication/streams").toFile());
//		Topology topology = runner.parseReactivePipeTopology(Paths.get("/Users/frank/git/dvdstore.replication").toFile(),Paths.get(this.storagePath));
		System.err.println("Topology: \n"+topology.describe());
	}
	
	@Test @Ignore
	public void runParseFolder() throws ParseException, IOException, InterruptedException {
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));

		KafkaStreams stream = runner.runPipeFolder(Paths.get("/Users/frank/git/dvdstore.replication").toFile());
		for (int i = 0; i < 50; i++) {
			boolean isRunning = stream.state().isRunning();
	        String stateName = stream.state().name();
	        System.err.println("State: "+stateName+" - "+isRunning);
	        final Collection<StreamsMetadata> allMetadata = stream.allMetadata();
	        System.err.println("meta: "+allMetadata);
			Thread.sleep(4000);
		}
		stream.close();
		Thread.sleep(1000);
	}
	
	@Test @Ignore
	public void testJoinTopic() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("jointopic.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}

	@Test @Ignore
	public void testConfigurationStreamInstance() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("address.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test @Ignore
	public void testRemoteJoin() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("remotejoin.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());

		runTopology(topology);
	}

	@Test @Ignore
	public void testSimpleTopic() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("simpletopic.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testFilms() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("films.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testActors() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("actor.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}

	
	@Test @Ignore
	public void testDebezium() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(new Topology(), getClass().getClassLoader().getResourceAsStream("sinkLog.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}


	
	@Test @Ignore
	public void testAddressTopic() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("address.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}

	@Test @Ignore
	public void testDemo1() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("demo1.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}


	@Test @Ignore
	public void testDemo3() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("demo3.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
	
	@Test @Ignore
	public void testDemo4() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("demo4.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
	
	@Test
	public void testPipesWithPartials() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("pipewithpartials.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	

	@Test
	public void testGroupBy() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("totalpayment.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}

	@Test @Ignore
	public void testParams() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(new Topology(),getClass().getClassLoader().getResourceAsStream("testparam.rr"),"junit");
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
}
