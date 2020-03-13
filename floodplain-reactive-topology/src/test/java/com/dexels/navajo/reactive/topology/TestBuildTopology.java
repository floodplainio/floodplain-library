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
	public void setup() throws IOException {
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		topologyContext = new TopologyContext(Optional.of("Generic"), "test", "someinstance", "20200310");
		String applicationId = topologyContext.applicationId();
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));
		runner = new TopologyRunner(storagePath,applicationId,sc,false);
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
//		runner.materializeConnectors();
	}
	


	@Test @Ignore
	public void testDatabase() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("database.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
//		runTopology(topology);
	}
	
	@Test @Ignore
	public void testStorelessTopic() throws ParseException, IOException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("simplewithoutstore.rr"));
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test @Ignore
	public void runParseFolder() throws ParseException, IOException, InterruptedException {
		StreamConfiguration sc = StreamConfiguration.parseConfig("test", getClass().getClassLoader().getResourceAsStream("resources.xml"));

		KafkaStreams stream = runner.runPipeFolder(topologyContext, Paths.get("/Users/frank/git/dvdstore.replication").toFile());
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
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("jointopic.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}

	@Test @Ignore
	public void testConfigurationStreamInstance() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("address.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
	}

	@Test @Ignore
	public void testRemoteJoin() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("remotejoin.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());

		runTopology(topology);
	}

	@Test @Ignore
	public void testSimpleTopic() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("simpletopic.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testFilms() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("films.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testActors() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("actor.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}

	
	@Test @Ignore
	public void testDebezium() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(), getClass().getClassLoader().getResourceAsStream("sinkLog.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}


	
	@Test @Ignore
	public void testAddressTopic() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("address.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}

	@Test @Ignore
	public void testDemo1() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("demo1.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}


	@Test @Ignore
	public void testDemo3() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("demo3.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
	
	@Test @Ignore
	public void testDemo4() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("demo4.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
	
	@Test @Ignore
	public void testPipesWithPartials() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("pipewithpartials.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	

	@Test
	public void testGroupBy() throws ParseException, IOException, InterruptedException {
		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("totalpayment.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);

		System.err.println("Topology: \n"+topology.describe());
		runner.materializeConnectors(topologyContext,true);
		runTopology(topology);
	}

	@Test @Ignore
	public void testCustomerWithTotal() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("customerwithtotals.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);
	}
	
	@Test @Ignore
	public void testParams() throws ParseException, IOException, InterruptedException {

		Topology topology = runner.parseSinglePipeDefinition(topologyContext, new Topology(),getClass().getClassLoader().getResourceAsStream("testparam.rr"));
		ReplicationTopologyParser.materializeStateStores(runner.topologyConstructor(), topology);
		System.err.println("Topology: \n"+topology.describe());
		runTopology(topology);

	}
}
