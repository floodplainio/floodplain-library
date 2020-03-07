package com.dexels.kafka.streams.processor.generic;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import org.apache.commons.text.StringSubstitutor;

import java.util.Optional;
import java.util.function.Function;

public class TransformPubSub implements Function<PubSubMessage, PubSubMessage> {
	private final String destinationTemplate;
	private final TopologyContext context;
	
	
	public TransformPubSub(String destinationTemplate, TopologyContext context) {
		this.destinationTemplate = destinationTemplate;
		this.context = context;
	}
	@Override
	public PubSubMessage apply(PubSubMessage in) {
		ReplicationMessage parsedMessage = ReplicationFactory.getInstance().parseBytes(Optional.empty(), in.value());
		String target = parseTemplateMessage(Optional.of(parsedMessage), destinationTemplate);
		return in.withTopic(Optional.of(target));
	}
	
	String parseTemplateMessage(Optional<ReplicationMessage> msg, String templateString) {
		
		StringSubstitutor sub = new StringSubstitutor(key -> {
			switch (key) {
				case "#deployment":
					return context.deployment;
				case "#instance":
					return context.instance;
//					instance.map(c1->c1.instanceName()).orElseThrow(()->new RuntimeException("Error parsing template string: "+templateString+" and key: "+key+" : No #instance present.")); 
				case "#generation":
					return  context.generation;
				default:
					return ""+msg.orElseThrow(()->new RuntimeException("Error parsing template string: "+templateString+" and key: "+key+" : No message references allowed here."))
							.columnValue(key);
			}
		});		
		final String replace = sub.replace(templateString);
		return CoreOperators.topicName(replace,context);
	}
}
