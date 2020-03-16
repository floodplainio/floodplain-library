package com.dexels.pubsub.rx2.api;

import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;

public interface PersistentSubscriber extends TopicSubscriber {
	public Publisher<List<PubSubMessage>> subscribe(String topic,String consumerGroup,boolean fromBeginning);
	public String encodeTopicTag(Map<Integer, Long> offsetMapInc);
	public Map<Integer, Long> partitionOffsets(String topic);
}
