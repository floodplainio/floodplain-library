package com.dexels.pubsub.rx2.api;


import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.util.function.Consumer;


/**
 * <p>
 * This is an example of an interface that is expected to be implemented by Providers of the API. Adding methods to this
 * interface is a minor change, because only Providers will be affected.
 * </p>
 * 
 * @see ProviderType
 * @since 1.0
 */
public interface MessagePublisher {
	public void publish(String key, byte[] value) throws IOException;
	public void publish(String key, byte[] value,Consumer<Object> onSuccess, Consumer<Throwable> onFail);
	public Subscriber<PubSubMessage> backpressurePublisher();
	public void flush();
	public void create();
	public void delete();

}
