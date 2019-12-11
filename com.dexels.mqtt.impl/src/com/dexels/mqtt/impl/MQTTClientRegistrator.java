package com.dexels.mqtt.impl;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.pubsub.rx2.api.MessagePublisher;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicPublisher;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.subscribers.DisposableSubscriber;

@Component(name="dexels.mqtt.registrator",configurationPolicy=ConfigurationPolicy.REQUIRE,immediate=true)
public class MQTTClientRegistrator implements TopicPublisher {

	private IMqttClient client;
	private ServiceRegistration<IMqttClient> registration;
	
	private final static Logger logger = LoggerFactory
			.getLogger(MQTTClientRegistrator.class);
	
	@Activate
	public void activate(final Map<String,Object> settings, final BundleContext bundleContext) {

		
		try {
			String uri = (String) settings.get("uri");
			String clientId = (String) settings.get("clientId");
			this.client = new MqttClient(uri,clientId);
			client.connect();
			if(bundleContext==null) {
				logger.warn("Not running in OSGi, should only happen in Unit Tests");
			} else {
				this.registration = bundleContext.registerService(IMqttClient.class, client,new Hashtable<>(settings) );
			}
			
		} catch (MqttSecurityException e) {
			logger.error("Error connecting MQTT",e);
		} catch (MqttException e) {
			logger.error("Error connecting MQTT",e);
		}
	}
	
	
	@Deactivate
	public void deactivate() {
		try {
			if(registration!=null) {
				registration.unregister();
				registration = null;
			}
			this.client.disconnect();
			this.client.close();
			this.client = null;
			logger.info("client closed.");
		} catch (MqttException e) {
			logger.error("Error: ", e);
		}
	}


	public IMqttClient getClient() {
		return client;
	}


	@Override
	public void publish(String topic, String key, byte[] value, Consumer<Object> onSuccess, Consumer<Throwable> onFail) {
		try {
			// keys aren't used here
			MqttMessage mm = new MqttMessage(value);
			client.publish(topic, mm);
			onSuccess.accept(mm);
		} catch (MqttException e) {
			onFail.accept(e);
		}
	}


	@Override
	public Subscriber<PubSubMessage> backpressurePublisher(Optional<String> defaultTopic, int maxInFlight) {
		return new DisposableSubscriber<PubSubMessage>() {


			@Override
			public void onComplete() {
				dispose();
			}

			@Override
			public void onError(Throwable e) {
				logger.error("Error: ", e);
				dispose();
			}

			@Override
			public void onNext(PubSubMessage p) {
					publish(p.topic().orElse(defaultTopic.orElseThrow(()->new RuntimeException("No topic in msg and also no default topic")))
							,""
							, p.value(),o->request(1),e->onError(e));
				
			}};
	}


	@Override
	public MessagePublisher publisherForTopic(String topic) {
		return new MessagePublisher() {
			
			@Override
			public void publish(String key, byte[] value, Consumer<Object> onSuccess, Consumer<Throwable> onFail) {
				MQTTClientRegistrator.this.publish(topic, key, value, onSuccess, onFail);
			}
			
			@Override
			public void publish(String key, byte[] value) throws IOException {
				MQTTClientRegistrator.this.publish(topic, key, value);
			}
			
			@Override
			public void flush() {
				// can safely be ignored
				
			}
			
			@Override
			public void delete() {
				// can safely be ignored
			}
			
			@Override
			public void create() {
				// can safely be ignored
				
			}
			
			@Override
			public Subscriber<PubSubMessage> backpressurePublisher() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}


	@Override
	public void publish(String topic, String key, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create(String topic) {
		// can safely be ignored
	}


	@Override
	public void delete(String topic) {
		// can safely be ignored
	}


	@Override
	public void create(String topic, Optional<Integer> replicationFactor, Optional<Integer> partitionCount) {
		// can safely be ignored
	}


	@Override
	public void flush() {
		// can safely be ignored
	}


	@Override
	public Flowable<String> streamTopics() {
		return Flowable.empty();
	}


	@Override
	public Completable deleteCompletable(List<String> topics) {
		return Completable.complete();
	}

}
