package com.dexels.kafka.impl;

import com.dexels.kafka.factory.KafkaClientFactory;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.reactivex.Flowable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


public class TestTagging {
	private KafkaTopicSubscriber kts;

	
	private final static Logger logger = LoggerFactory.getLogger(TestTagging.class);

	@Before
	public void setup() {
		Map<String,String> config = new HashMap<>();
		config.put("wait", "5000");
		config.put("max", "5");
		kts = (KafkaTopicSubscriber) KafkaClientFactory.createSubscriber(System.getenv("KAFKA_DEVELOP"), config);
//		KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 1, 1)
	}
	
	@Test(timeout=60000)  @Ignore
	public void testCreatingAndSingleTagging() throws InterruptedException {
		int count = 4000;
		ReplicationFactory.setInstance(new FallbackReplicationMessageParser(true));
		final String topic = "KNVB-develop-sportlinkkernel-PERSON";
		List<String> persons = Flowable.fromPublisher(kts.subscribeSingleRange(topic, UUID.randomUUID().toString(), "0:0", "0:"+count))
				.concatMap(l->Flowable.fromIterable(l))
				.map(e->ReplicationFactory.getInstance().parseBytes(Optional.of(topic), e.value()))
				.doOnNext(e->e.commit())
				.map(e->(String)e.columnValue("lastname"))
				.toList()
				.blockingGet();
				
		Assert.assertEquals(count, persons.size());
	}
	
	@Test (timeout=60000)
	@Ignore
	public void testRange() throws InterruptedException {
		ReplicationFactory.setInstance(new FallbackReplicationMessageParser(true));
		final String topic = "testtopic";
		List<String> persons = Flowable.fromPublisher(kts.subscribeSingleRange(topic, "testTag", "0:2,1:2,2:2", "0:5,1:5,2:5"))
				.flatMap(l->Flowable.fromIterable(l))
				.map(e->ReplicationFactory.getInstance().parseBytes(Optional.of(topic), e.value()))
				.map(e->(String)e.columnValue("name"))
				.toList()
				.blockingGet();
				
		Assert.assertEquals(9, persons.size());
	}
	
	@Test (timeout=60000)
	@Ignore
	public void testCreatingAndTagging() throws InterruptedException {
		TopicPublisher tp = KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 3, 1);
		String temporaryTopic = "temptopic-"+UUID.randomUUID().toString();
		int partitionCount = 5;
		logger.info("Using temp topic: "+temporaryTopic+" partitions: "+partitionCount);
		
		tp.create(temporaryTopic,Optional.of(1),Optional.of(partitionCount));
		try {
			Map<Integer,Long> result = kts.partitionOffsets(temporaryTopic);
			long sum = result.entrySet().stream().map(e->e.getValue()).collect(Collectors.summarizingLong(e->e)).getSum();
			logger.info("result: "+result);
			Assert.assertEquals(partitionCount, result.size());
			Assert.assertEquals(partitionCount, sum);
			long messageCount = 1000;
			LongStream.range(0, messageCount).forEach(i->{
				try {
					tp.publish(temporaryTopic, "key"+i, ("somevalue"+i).getBytes());
				} catch (Exception e) {
				}
				
			});
			tp.flush();
			final Map<Integer, Long> offsets = kts.partitionOffsets(temporaryTopic);
			logger.info("Offsets now: "+offsets);
			long sumAfter = offsets
					.entrySet()
					.stream()
					.map(e->e.getValue())
					.collect(Collectors.summarizingLong(e->e)).getSum();
			logger.info("Summary: "+sumAfter+" -> "+messageCount);
			Assert.assertEquals(messageCount+partitionCount, sumAfter);
			
		} finally {
			tp.delete(temporaryTopic);
		}
	}


	@Test (timeout=60000)
	@Ignore
	public void testOffsetReading() throws InterruptedException {
		TopicPublisher tp = KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 3, 1);
		String temporaryTopic = "temptopic-"+UUID.randomUUID().toString();
		int partitionCount = 3;
		long messageCount = 10;
		tp.create(temporaryTopic,Optional.of(1),Optional.of(partitionCount));
		final Map<Integer, Long> beforeOffsets = kts.partitionOffsets(temporaryTopic);
		long totalBefore = sumOffsets(beforeOffsets);
		// Assert that every offset starts at one, so total offset equals = nr. of partitions
		Assert.assertEquals(0, totalBefore);
		logger.info("BEF: "+beforeOffsets);
		try {
			IntStream.range(0, 5).forEach(i->{
				publishMessages(tp, temporaryTopic, messageCount);
				tp.flush();
				final Map<Integer, Long> offsets = kts.partitionOffsets(temporaryTopic);
				logger.info("Offset of iteration: "+i+" :: "+offsets+" -> sum offsets: "+sumOffsets(offsets));
				Assert.assertEquals(messageCount * (i+1), sumOffsets(offsets));
			});
		} finally {
			tp.delete(temporaryTopic);
		}
	}

	@Test (timeout=60000)
	@Ignore
	public void testTagEncode() throws InterruptedException {
		TopicPublisher tp = KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 3, 1);
		String temporaryTopic = "temptopic-"+UUID.randomUUID().toString();
		int partitionCount = 2;
		tp.create(temporaryTopic,Optional.of(1),Optional.of(partitionCount));
		try {
			tp.flush();
			final Map<String,Map<Integer, Long>> offsets = kts.offsets(Arrays.asList(new String[]{temporaryTopic}));
			String s =  kts.encodeTag(offsets);
			logger.info(":::::: "+s);
		} finally {
			tp.delete(temporaryTopic);
		}
	}
	
	@Test (timeout=60000)
	@Ignore
	public void testBasicTag() throws InterruptedException {
		TopicPublisher tp = KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 3, 1);
		String temporaryTopic = "temptopic-"+UUID.randomUUID().toString();
		String temporaryGroup = "tempgroup-basic-"+UUID.randomUUID().toString();
		int partitionCount = 2;
		long messageCount = 10;
		tp.create(temporaryTopic,Optional.of(1),Optional.of(partitionCount));
		try {
			publishMessages(tp, temporaryTopic, messageCount);
			tp.flush();
			final Map<Integer, Long> offsets = kts.partitionOffsets(temporaryTopic);
			
			publishMessages(tp, temporaryTopic, messageCount);
			tp.flush();
			final Map<Integer, Long> afterOffsets = kts.partitionOffsets(temporaryTopic);
			publishMessages(tp, temporaryTopic, messageCount);
			tp.flush();
			
			Map<String,Map<Integer,Long>> fromOffsets = new HashMap<>();
			fromOffsets.put(temporaryTopic, offsets);
			Map<String,Map<Integer,Long>> toOffsets = new HashMap<>();
			toOffsets.put(temporaryTopic, afterOffsets);
			logger.info("fromOFFSET: "+offsets);
			logger.info("toOFFSET: "+afterOffsets);
			
			long p = Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{temporaryTopic}), temporaryGroup, Optional.of((topic,partition)->fromOffsets.get(topic).get(partition)),  Optional.of((topic,partition)->toOffsets.get(topic).get(partition)), Optional.empty(), false, ()->{}))
					.flatMap(l->Flowable.fromIterable(l))
					.map(pb->pb.partition().get()+" - "+pb.offset().get()+" -> "+toOffsets)
					.count()
					.blockingGet();
			logger.info("Should be: "+offsets+" to: "+afterOffsets+" items: "+p);
//			for (String msg : msglist) {
//				logger.info("MessageList: "+msg);
//				
//			}
//			logger.info("Count: "+count);
			
			Assert.assertEquals(messageCount, p);
		} finally {
			tp.delete(temporaryTopic);
		}
	}
	
	
	@Test (timeout=60000)
	@Ignore
	public void testMultiTagging() throws InterruptedException {
		TopicPublisher tp = KafkaClientFactory.createPublisher(System.getenv("KAFKA_DEVELOP"), 3, 1);
		String temporaryTopic = "temptopic-"+UUID.randomUUID().toString();
		String temporaryGroup = "tempgroup-"+UUID.randomUUID().toString();
		int partitionCount = 1;
		logger.info("Using temp topic: "+temporaryTopic+" partitions: "+partitionCount);
		
		tp.create(temporaryTopic,Optional.of(1),Optional.of(partitionCount));
		try {
			Map<Integer,Long> initialOffsets = kts.partitionOffsets(temporaryTopic);
			long sum = initialOffsets.entrySet().stream().map(e->e.getValue()).collect(Collectors.summarizingLong(e->e)).getSum();
			logger.info("initial: "+initialOffsets);
			Assert.assertEquals(partitionCount, initialOffsets.size());
			Assert.assertEquals(partitionCount, sum);
			long messageCount = 10;
			publishMessages(tp, temporaryTopic, messageCount);
			logger.info("Published: "+messageCount+" messages");
			tp.flush();
			logger.info("... and flushed");
			final Map<Integer, Long> offsets = kts.partitionOffsets(temporaryTopic);

			logger.info("Offsets now: "+offsets);
			long sumAfter = sumOffsets(offsets);
			logger.info("Summary: "+sumAfter+" -> "+messageCount);
			Assert.assertEquals(messageCount, sumAfter);

			publishMessages(tp, temporaryTopic, messageCount);
			logger.info("Published another: "+messageCount+" messages");
			tp.flush();
			logger.info("... and flushed");

			final Map<Integer, Long> finalOffsets = kts.partitionOffsets(temporaryTopic);
			publishMessages(tp, temporaryTopic, messageCount);
			logger.info("Published another (2): "+messageCount+" messages");

			tp.flush();
			logger.info("... and flushed");
			logger.info("Before: "+offsets+" total: "+sumOffsets(offsets)+" After: "+finalOffsets+" total: "+sumOffsets(finalOffsets));
			Map<String,Map<Integer,Long>> fromOffsets = new HashMap<>();
			fromOffsets.put(temporaryTopic, offsets);
			Map<String,Map<Integer,Long>> toOffsets = new HashMap<>();
			toOffsets.put(temporaryTopic, finalOffsets);
			
			List<String> msglist = Flowable.fromPublisher(kts.subscribe(Arrays.asList(new String[]{temporaryTopic}), temporaryGroup, Optional.of((topic,partition)->fromOffsets.get(topic).get(partition)),  Optional.of((topic,partition)->toOffsets.get(topic).get(partition)), Optional.empty(), false, ()->{}))
				.flatMap(l->Flowable.fromIterable(l))
				.map(pb->pb.topic().get()+" - "+pb.partition().get()+" - "+pb.offset().get())
				.toList()
				.blockingGet();
			logger.info("Size: "+msglist);
			logger.info("from: "+fromOffsets+" to: "+toOffsets);
			Assert.assertEquals(messageCount*2, sumOffsets(finalOffsets));
			
		} finally {
			tp.delete(temporaryTopic);
		}
	}

	private void publishMessages(TopicPublisher tp, String temporaryTopic, long messageCount) {
		LongStream.range(0, messageCount).forEach(i->tp.publish(temporaryTopic, "key"+(i+messageCount), (""+(i+messageCount)).getBytes(),a->{},e->{}));
//		tp.flush();
	}

	private long sumOffsets(Map<Integer, Long> offsets) {
		final long size = offsets
				.entrySet()
				.stream()
//				.map(e->e.getValue())
				// subtract one:
				.collect(Collectors.summarizingLong(e->e.getValue()-1)).getSum();
		logger.info("Offsets now: "+offsets+" total: "+size);
		return size;

	}
}
