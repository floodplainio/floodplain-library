package com.dexels.pubsub.rx2.api;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface TopicPublisher {
    @Deprecated
    public MessagePublisher publisherForTopic(String topic);

    public void publish(String topic, String key, byte[] value) throws IOException;

    public void publish(String topic, String key, byte[] value, Consumer<Object> onSuccess, Consumer<Throwable> onFail);

    // should be backpressureSubscriber, right?
    public Subscriber<PubSubMessage> backpressurePublisher(Optional<String> defaultTopic, int maxInFlight);

    public void create(String topic, Optional<Integer> replicationFactor, Optional<Integer> partitionCount);

    public void create(String topic);

    public void delete(String topic);

    public void flush();

    public Flowable<String> streamTopics();

    public Completable deleteCompletable(List<String> topics);
}
