package com.dexels.kafka.streams.testdata;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

//import kafka.consumer.ConsumerConfig;

public class TestStream {
	
	private final static String generation = "gen-43t";

	private final static Optional<String> tenant = Optional.of("KNBSB");
	private final static String deployment = "develop";
	private final static String instance = "testsetup";
	
	private static final TopologyContext context = new TopologyContext(tenant, deployment, instance, generation);
	static {
		ReplicationFactory.setInstance(new JSONReplicationMessageParserImpl());
	}
	public static ValueJoiner<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> multiValueJoiner(String field,List<String> ignore) {
		return  (core,added)->joinReplication(core,added,field,ignore);
	}

	public static ReplicationMessage joinReplication(ReplicationMessage core, List<ReplicationMessage> added, String sub, List<String> ignore) {
		if(added!=null) {
//			logger.info("Joining list to core: "+core.queueKey()+" size: "+added.size());
			core = core.withSubMessages(sub, added.stream().map(m->m.message().without(ignore)).collect(Collectors.toList()));
		}
		return core;
	}	
	


}
