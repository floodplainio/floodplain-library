package com.dexels.kafka.streams.processor.generic;

import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.base.StreamConfiguration;
import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;

public class TransformPubSub implements Function<PubSubMessage, PubSubMessage> {
	private final String destinationTemplate;
	private final Optional<StreamInstance> instance;
	private final StreamConfiguration config;
	private final Optional<String> tenant;
	
	
	public TransformPubSub(String destinationTemplate,Optional<StreamInstance> instance, Optional<String> tenant) {
		this.destinationTemplate = destinationTemplate;
		this.instance = instance;
		this.tenant = tenant;
		this.config = instance.map(i->i.getConfig()).orElseThrow(()->new RuntimeException("No instance present")).orElseThrow(()->new RuntimeException("No config present"));
	}
	@Override
	public PubSubMessage apply(PubSubMessage in) {
		ReplicationMessage parsedMessage = ReplicationFactory.getInstance().parseBytes(Optional.empty(), in.value());
		String target = parseTemplateMessage(Optional.of(parsedMessage), destinationTemplate);
		return in.withTopic(Optional.of(target));
	}
	
	String parseTemplateMessage(Optional<ReplicationMessage> msg, String templateString) {
		
		StringSubstitutor sub = new StringSubstitutor(new StringLookup() {
			@Override
			public String lookup(String key) {
				switch (key) {
					case "#deployment":
						return config.deployment();
					case "#instance":
						return instance.map(c->c.instanceName()).orElseThrow(()->new RuntimeException("Error parsing template string: "+templateString+" and key: "+key+" : No #instance present.")); 
					case "#generation":
						return instance.map(c->c.generation()).orElseThrow(()->new RuntimeException("Error parsing template string: "+templateString+" and key: "+key+" : No #generation present."));
					default:
						return ""+msg.orElseThrow(()->new RuntimeException("Error parsing template string: "+templateString+" and key: "+key+" : No message references allowed here."))
								.columnValue(key);
				}
			}
		});		
		final String replace = sub.replace(templateString);
		return CoreOperators.topicName(replace,new TopologyContext(tenant, config.deployment(), instance.map(i->i.instanceName()).orElse(""), instance.map(e->e.generation()).orElse("nodeploy")));
	}
}
