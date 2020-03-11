package com.dexels.pubsub.rx2.api;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;

public interface TopicSubscriber {
	public default Publisher<List<PubSubMessage>> subscribe(List<String> topics, String consumerGroup, Optional<BiFunction<String,Integer, Long>> fromTag, Optional<BiFunction<String, Integer, Long>> toTag, Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned) {
		return subscribe(topics, consumerGroup, fromTag, toTag, clientId,fromBeginning,onPartitionsAssigned,true);
	}
	public Publisher<List<PubSubMessage>> subscribe(List<String> topics,String consumerGroup,Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned);
//	public Publisher<List<PubSubMessage>> subscribeSingle(String topic,String consumerGroup,Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned);
	public Publisher<List<PubSubMessage>> subscribe(List<String> topics, String consumerGroup, Optional<BiFunction<String,Integer, Long>> fromTag, Optional<BiFunction<String, Integer, Long>> toTag, Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned, boolean commitBeforeEachPoll);
	public Publisher<List<PubSubMessage>> subscribeSingleRange(String topic, String consumerGroup, String fromTag, String toTag);
	public Publisher<List<PubSubMessage>> subscribe(String topic,String consumerGroup,boolean fromBeginning);
}
