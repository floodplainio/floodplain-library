package com.dexels.kafka.streams.debezium.impl;

import java.util.Optional;

import org.osgi.service.component.annotations.Component;

import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.debezium.DebeziumComponent;
import com.dexels.kafka.streams.processor.generic.GenericProcessor;
import com.dexels.kafka.streams.processor.generic.GenericProcessorBuilder;
import com.dexels.kafka.streams.xml.parser.XMLElement;

@Component(name="dexels.debezium.processor",property={"name=dexels.debezium.processor"})
public class DebeziumProcessor implements GenericProcessorBuilder {

	@Override
	public GenericProcessor build(XMLElement element, Optional<StreamInstance> streamInstance, Optional<String> tenant) {
		String source = element.getStringAttribute("source");
		String group = element.getStringAttribute("group");
		String destination = element.getStringAttribute("destination");
		boolean appendTenant = false;
		boolean appendSchema = false;
		DebeziumComponent component = new DebeziumComponent(group,source,destination,streamInstance.get(),tenant,streamInstance.get().createPublisher(),streamInstance.get().createSubscriber(),appendTenant,appendSchema);
		return ()->component.disposable().dispose();
	}

}
