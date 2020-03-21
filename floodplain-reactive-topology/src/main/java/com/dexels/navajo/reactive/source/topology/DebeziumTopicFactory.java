package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

import java.util.*;

public class DebeziumTopicFactory implements ReactiveSourceFactory {

	@Override
	public Type sourceType() {
		return Type.MESSAGE;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList("table","schema","resource","appendSchema"));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList("table","schema","resource"));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("table",ImmutableMessage.ValueType.STRING,"schema", ImmutableMessage.ValueType.STRING,"resource", ImmutableMessage.ValueType.STRING,"appendSchema", ImmutableMessage.ValueType.BOOLEAN));
	}


	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new DebeziumTopic(this,parameters);
	}

	@Override
	public String name() {
		return "debezium";
	}



}
