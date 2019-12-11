package com.dexels.kafka.webapi;

import java.util.Map;

import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.dexels.server.mgmt.api.ServerHealthCheck;

@Component(name="dexels.kafka.infracheck")
public class KafkaInfraCheck implements ServerHealthCheck {

	private PersistentSubscriber persistentSubscriber;
	private PersistentPublisher publisher;

	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearTopicPublisher")
	public void setTopicPublisher(PersistentPublisher publisher) {
		this.publisher = publisher;
	}
	
	
	public void clearTopicPublisher(TopicPublisher publisher) {
		this.publisher = null;
	}

	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearPersistentSubscriber")
	public void setPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = persistenSubscriber;
	}
	
	public void clearPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = null;
	}
	
	@Activate
	protected synchronized void activate(BundleContext bundleContext, Map<String,Object> settings) {
		// noop
	}
	@Override
	public boolean isOk() {
		return persistentSubscriber!=null && publisher!=null;
	}

	@Override
	public String getDescription() {
		String subNull = persistentSubscriber==null? "No persistent subscriber":"";
		String pubNull = publisher==null? "No persistent publisher":"";
		return String.join("\n", subNull,pubNull);
	}

}
