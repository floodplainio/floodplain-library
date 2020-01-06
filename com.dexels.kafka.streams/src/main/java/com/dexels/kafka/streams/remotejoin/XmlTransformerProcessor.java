package com.dexels.kafka.streams.remotejoin;

import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;

import com.dexels.kafka.streams.base.Filters;
import com.dexels.kafka.streams.base.StreamOperators;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.transformer.api.MessageTransformer;

public class XmlTransformerProcessor extends AbstractProcessor<String, ReplicationMessage> implements Punctuator {

	private final MessageTransformer myTransformer;
	Predicate<String, ReplicationMessage> associationFilter;
	
	public XmlTransformerProcessor(Optional<XMLElement> elt, String sourceTopicName, TopologyConstructor topologyConstructor) {
		this.myTransformer = StreamOperators.transformersFromChildren(elt, topologyConstructor.transformerRegistry, sourceTopicName).orElse((params,msg)->msg);
		if (elt.isPresent()) {
			this.associationFilter = Filters.getFilter(Optional.ofNullable(elt.get().getStringAttribute("filter"))).orElse((key,value)->true);
		} else {
			this.associationFilter = (key,value)->true;
		}
	}

	@Override
	public void init(ProcessorContext context) {
		super.init(context);
	}

	
	
	@Override
	public void punctuate(long timestamp) {
//		logger.info("XML transformer updater did: {} updates timestamp: {}",updateCounter.longValue(),timestamp);
	}

	@Override
	public void process(String key, ReplicationMessage outerMessage) {
		if(outerMessage==null) {
			context().forward(key, outerMessage);
			return;
		}
		ReplicationMessage result = myTransformer.apply(Collections.emptyMap(), outerMessage);
		if(this.associationFilter.test(key, result)) {
			context().forward(key, result);
		}
	}

}
