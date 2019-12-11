package com.dexels.kafka.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.kafka.factory.KafkaClientFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

import io.reactivex.Flowable;

public class TestSubscriber {
	private KafkaTopicSubscriber kts;
	@Before
	public void setup() {
		Map<String,String> config = new HashMap<>();
//		config.put("wait", "5000");
//		config.put("max", "50");
		final String server = System.getenv("KAFKA_DEVELOP");
		System.err.println("Connecting to server: "+server);
		kts = (KafkaTopicSubscriber) KafkaClientFactory.createSubscriber(server, config);
	}

	@Test  @Ignore
	public void testTopicList() {
		List<String> l = kts.topics();
		System.err.println("Topics: >>>> "+l.size()+" <<<<");
		Assert.assertTrue("There should be topics",l.size()>1);
	}

	@Test @Ignore
	public void testOffsetQuery() {
		Map<Integer,Long> l = kts.partitionOffsets("NAVAJO-WORKFLOW-EVENT-develop");
		System.err.println(">>>> "+l+" <<<<");
		
	}
	@Test @Ignore
	public void testTimestamps() {
		if(System.getenv("KAFKA_DEVELOP")==null) {
			return;
		}
		AtomicLong count = new AtomicLong();

		try {
			
			System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
			
			ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
			ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
//			AtomicLong requestCount = new AtomicLong();
			String topic = "testtopic";
			Flowable<byte[]> o = Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{topic}), "zzy", Optional.empty(), true,()->{})) 
					.concatMapIterable(e->e)
					.doOnNext(e->count.incrementAndGet())
					.retry(5)
					.map(e->parser.parseBytes(Optional.of(topic), e.value()))
					.map(e->jsonparser.serialize(e));

			File dump = new File("dump.xml");
			FileOutputStream fos = new FileOutputStream(dump);
			o.blockingForEach(e->{
				fos.write(e);
				count.incrementAndGet();
			});
			fos.close();

		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.err.println("I'm done!");
		System.err.println("Count: "+count.get());

		
	}
	
	@Test(timeout=10000) @Ignore
	public void testExample() {
		if(System.getenv("KAFKA_DEVELOP")==null) {
			return;
		}
		AtomicLong count = new AtomicLong();

		try {
			
			System.setProperty(ReplicationMessage.PRETTY_JSON, "true");
			
			ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
			ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
//			AtomicLong requestCount = new AtomicLong();
//			StreamOper
			String topic = "testtopic-COMPETITIONTYPEWITHAGECLASS";
			String columns = "ageclasscode,ageclassname,competitionkind,competitiontypename,duration,extratime,gender,result_processing_method,isacategory,result_filter,series,sportid,sporttag,mandatorycoordinator,mandatoryassistants,mandatorysecretary,allowsassignmentbyclub,allowsmobiledwf,filloutbysecretary,fieldsize,availabledaysbefore,best_player_voting,mandatorygoaltakers";
			List<String> ll = Arrays.asList(columns.split(","));
			Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{topic}),UUID.randomUUID().toString(), Optional.empty(), true,()->{})) 
//					.filter(e->e.size()>0)
					.map(e->Flowable.fromIterable(e))
					.concatMap(e->e)
//					.concatMapIterable(e->e)
//					.doOnNext(e->count.incrementAndGet())
//					.retry(5)
					.map(e->parser.parseBytes(Optional.of(e.topic().orElse(topic)), e.value()))
					.map(e->e.withOnlyColumns(ll))
					.map(e->jsonparser.serialize(e))
					.doOnError(e->e.printStackTrace())
					.blockingForEach(e->{
						System.err.println("Item: "+new String(e));
					});
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.err.println("I'm done!");
		System.err.println("Count: "+count.get());

		
	}
}
