package com.dexels.kafka.streams.processor.generic;

import java.util.Optional;

import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.xml.parser.XMLElement;

public interface GenericProcessorBuilder {
	public GenericProcessor build(XMLElement element, Optional<StreamInstance> streamInstance, Optional<String> tenant);
}
