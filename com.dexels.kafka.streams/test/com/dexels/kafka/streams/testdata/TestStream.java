package com.dexels.kafka.streams.testdata;
import static com.dexels.kafka.streams.api.CoreOperators.addToReplicationList;
import static com.dexels.kafka.streams.api.CoreOperators.extractKey;
import static com.dexels.kafka.streams.api.CoreOperators.generationalGroup;
import static com.dexels.kafka.streams.api.CoreOperators.removeFromReplicationList;
import static com.dexels.kafka.streams.api.CoreOperators.topicName;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamConfiguration;
import com.dexels.kafka.streams.base.StreamOperators;
import com.dexels.kafka.streams.base.StreamRuntime;
import com.dexels.kafka.streams.xml.parser.CaseSensitiveXMLElement;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.mongodb.sink.RunKafkaConnect;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;

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
