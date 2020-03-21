package com.dexels.navajo.reactive.source.test;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EventStreamSourceFactory implements ReactiveSourceFactory {

	public EventStreamSourceFactory() {
	}

	@Override
	public Type sourceType() {
		return Type.EVENTSTREAM;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"classpath"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("classpath",ImmutableMessage.ValueType.STRING));
	}

	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
//		problems.add(ReactiveParseProblem.of("event source is missing a source ('classpath' only supported now)"));

		return new EventStreamSource(this,parameters);
	}

	@Override
	public String name() {
		return "eventsource";
	}

}
