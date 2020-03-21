package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TopicSourceFactory implements ReactiveSourceFactory {


	public TopicSourceFactory() {
	}

	@Override
	public Type sourceType() {
		return Type.MESSAGE;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList("name"));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList("name"));
}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("string",ImmutableMessage.ValueType.STRING));
	}

	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new TopicSource(this,parameters);
	}

	@Override
	public String name() {
		return "topic";
	}



}
