package com.dexels.kafka.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.dexels.kafka.factory.KafkaClientFactory;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public class TopicDump {
	public static Flowable<byte[]> downloadTopic(String bootstrapServer, String topic, Predicate<ReplicationMessage> filter, Function<ReplicationMessage,ReplicationMessage> map, Optional<String> from, Optional<String> to) throws IOException {

		System.setProperty(ReplicationMessage.PRETTY_JSON, "true");

		ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
		ReplicationMessageParser jsonparser = new JSONReplicationMessageParserImpl();
		
		Map<String,String> config = new HashMap<>();
		KafkaTopicSubscriber kts = (KafkaTopicSubscriber) KafkaClientFactory.createSubscriber(bootstrapServer, config);
		Map<Integer, Long> offsetMap = kts.partitionOffsets(topic);
		Map<Integer, Long> offsetMapInc = new HashMap<Integer, Long>();
		offsetMap.entrySet().forEach(e->{
			offsetMapInc.put(e.getKey(), e.getValue()-1);
		});
		String toTag = to.orElseGet(()->kts.encodeTopicTag( offsetMapInc));
		// TODO multiple partitions
		String fromTag = from.orElse("0:0");
		AtomicLong messageCount = new AtomicLong();
		AtomicLong writtenDataCount = new AtomicLong();
		AtomicLong writtenMessageCount = new AtomicLong();

		final Disposable d = Flowable.interval(10, TimeUnit.SECONDS)
			.doOnNext(e->System.err.println("In progress. MessageCount: "+messageCount.get()+" writtenMessageCount: "+writtenMessageCount+" written data: "+writtenDataCount.get()))
			.doOnTerminate(()->System.err.println("Progress complete"))
			.subscribe();

		
		return Flowable.fromPublisher(kts.subscribeSingleRange(topic, UUID.randomUUID().toString(), fromTag, toTag))
			.concatMapIterable(e->e)
			.doOnNext(m->messageCount.incrementAndGet())
			.retry(5)
			.filter(e->e.value()!=null)
			.map(e->parser.parseBytes(Optional.of(topic), e.value())
					.atTime(e.timestamp())
					.with("_kafkapartition", e.partition().orElse(-1), "integer")
					.with("_kafkaoffset", e.offset().orElse(-1L), "long")
					.with("_kafkakey", e.key(), "string") 
				)
			.filter(filter)
			.map(map)
			.doFinally(()->{d.dispose();
				System.err.println("Disposing progress monitor");
			})
			.map(e->jsonparser.serialize(e))
			.doOnNext(e->{
				writtenMessageCount.incrementAndGet();
				writtenDataCount.addAndGet(e.length);
			});
	}
	
	
	public static void dumpTopicToFile(String bootstrapServer, String path, String topic, Predicate<ReplicationMessage> filter, Function<ReplicationMessage,ReplicationMessage> map, Optional<String> from, Optional<String> to) throws IOException {
		File dump = new File(path);
		FileOutputStream fos = new FileOutputStream(dump);
		downloadTopic(bootstrapServer,topic,filter,map,from,to).blockingForEach(e->{
			fos.write(e);
		});
		fos.close();
	}

}
