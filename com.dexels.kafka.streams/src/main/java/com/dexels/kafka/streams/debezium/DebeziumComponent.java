package com.dexels.kafka.streams.debezium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;

import io.reactivex.disposables.Disposable;

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
