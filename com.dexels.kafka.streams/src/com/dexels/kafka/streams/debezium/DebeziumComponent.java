package com.dexels.kafka.streams.debezium;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamConfiguration;
import com.dexels.kafka.streams.processor.generic.TransformPubSub;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.dexels.pubsub.rx2.api.TopicSubscriber;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.DisposableSubscriber;

public class DebeziumComponent {

	private Disposable disposable;
	
	private static final Logger logger = LoggerFactory.getLogger(DebeziumComponent.class);

	public DebeziumComponent(String group, String topic, String destination, StreamConfiguration config, TopologyContext context, boolean appendTenant, boolean appendSchema) {
//		Optional<String> id = Optional.of(UUID.randomUUID().toString());
//		Set<String> createdTopics = new HashSet<>();
//		final DisposableSubscriber<PubSubMessage> backpressurePublisher = (DisposableSubscriber<PubSubMessage>)publisher.backpressurePublisher(Optional.empty(),500);
//		String deployment = config.deployment();
//		String resolvedGroup = CoreOperators.nonGenerationalGroup(group, context);
//		String resolvedTopic = CoreOperators.topicName(topic, context);
//		TransformPubSub psTransform = new TransformPubSub(destination, config,context);
//		Flowable.fromPublisher(subscriber.subscribe(Arrays.asList(resolvedTopic) ,resolvedGroup,id, true,()->{}))
//			.concatMapIterable(e->e)
//			.doOnSubscribe(s->logger.info("Registering to topic: {} with group: {}",resolvedTopic,resolvedGroup))
//			.subscribeOn(Schedulers.io())
//			.map(m->JSONToReplicationMessage.parse(m,appendTenant,appendSchema))
//			.filter(e->e.value()!=null)
//			.map(psTransform::apply)
//			.doOnNext(msg->{
//				if(msg.topic().isPresent()) {
//					String messageTopic = msg.topic().get();
//					try {
//						if(!createdTopics.contains(messageTopic)) {
//							createdTopics.add(messageTopic);
//							publisher.create(messageTopic);
//						}
//					} catch (Throwable e1) {
//						logger.debug("Error creating topic, might not be an issue (sometimes race condition on creation)",e1);
//					}
//				}
//			}).subscribe(backpressurePublisher);
//		this.disposable = backpressurePublisher;
	}
	
//	private void 
	
	public Disposable disposable() {
		return this.disposable;
	}


}
