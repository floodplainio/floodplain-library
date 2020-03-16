package com.dexels.kafka.webapi;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TopicDump {
	
	private static final Logger logger = LoggerFactory.getLogger(TopicDump.class);

	private TopicDump() {
		// no instance
	}
	public static Flowable<ReplicationMessage> downloadTopic(PersistentSubscriber kts,PersistentPublisher pub, String topic,
			Predicate<ReplicationMessage> filter, Function<ReplicationMessage, ReplicationMessage> map,
			Optional<String> from, Optional<String> to) {
		ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
		return downloadTopicRaw(kts, pub, topic, from, to)
				.map(e->parser.parseBytes(e))
				.filter(filter)
				.map(map);
	}
	public static Flowable<PubSubMessage> downloadTopicRaw(PersistentSubscriber kts,PersistentPublisher pub, String topic,
			Optional<String> from, Optional<String> to) {


		Map<Integer, Long> offsetMap = kts.partitionOffsets(topic);
		Map<Integer, Long> offsetMapInc = new HashMap<>();
		offsetMap.entrySet().forEach(e -> offsetMapInc.put(e.getKey(), e.getValue() - 1));
		String toTag = to.orElseGet(() -> kts.encodeTopicTag(offsetMapInc));
		String fromTag = from.orElse("0:0");
		AtomicLong messageCount = new AtomicLong();
		AtomicLong writtenMessageCount = new AtomicLong();

		final Disposable d = Flowable.interval(2, TimeUnit.SECONDS)
				.doOnNext(e -> logger.info("In progress. MessageCount: {} writtenMessageCount: {}",messageCount.get(), writtenMessageCount))
				.doOnTerminate(() -> logger.info("Progress complete: MessageCount: {} writtenMessageCount: {}",messageCount.get(), writtenMessageCount))
				.subscribe();
		final String generatedConsumerGroup = UUID.randomUUID().toString();
		Action onTerminate = ()->{
			d.dispose();
			pub.deleteGroups(Arrays.asList(generatedConsumerGroup));
		};
		
		return Flowable.fromPublisher(kts.subscribeSingleRange(topic, generatedConsumerGroup, fromTag, toTag))
				.concatMapIterable(e -> e)
				.doOnNext(m -> messageCount.incrementAndGet())
				.retry(5)
				.doOnTerminate(() -> logger.info("Progress complete: MessageCount: {} writtenMessageCount: {} ",messageCount.get(),writtenMessageCount))
				.doOnTerminate(onTerminate)
				.doOnCancel(onTerminate)
				.doOnNext(e->writtenMessageCount.incrementAndGet());


	}

	public static Flowable<String> downloadTopicKeys(PersistentSubscriber kts, String topic,
			Predicate<ReplicationMessage> filter, Function<ReplicationMessage, ReplicationMessage> map,
			Optional<String> from, Optional<String> to) {
		System.setProperty(ReplicationMessage.PRETTY_JSON, "true");

		ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);

		Map<Integer, Long> offsetMap = kts.partitionOffsets(topic);
		Map<Integer, Long> offsetMapInc = new HashMap<>();
		offsetMap.entrySet().forEach(e -> {
			offsetMapInc.put(e.getKey(), e.getValue() - 1);
		});
		String toTag = to.orElseGet(() -> kts.encodeTopicTag(offsetMapInc));
		// TODO multiple partitions
		String fromTag = from.orElse("0:0");
		AtomicLong messageCount = new AtomicLong();
		AtomicLong writtenDataCount = new AtomicLong();
		AtomicLong writtenMessageCount = new AtomicLong();

		final Disposable d = Flowable.interval(2, TimeUnit.SECONDS)
				.doOnNext(e -> logger.info("In progress. MessageCount: {} writtenMessageCount: {} written data: {}",messageCount.get(),writtenMessageCount,writtenDataCount.get()))
				.doOnTerminate(() -> logger.info("Progress complete")).subscribe();

		return Flowable.fromPublisher(kts.subscribeSingleRange(topic, UUID.randomUUID().toString(), fromTag, toTag))
				.concatMapIterable(e -> e).doOnNext(m -> messageCount.incrementAndGet()).retry(5)
				.doOnNext(e->{if(e.value()!=null)  System.err.println("Bytes detected: "+e.value().length); else System.err.println("null msg");})
				.map(parser::parseBytes)
				.doOnNext(e->logger.info("-> {}", ReplicationFactory.getInstance().describe(e)))
				.filter(filter)
				.map(ReplicationMessage::combinedKey)
				.doOnTerminate(() -> logger.info(">>>>>Progress complete"))
				.doOnTerminate(d::dispose)
				.doOnCancel(d::dispose);
	}
}
