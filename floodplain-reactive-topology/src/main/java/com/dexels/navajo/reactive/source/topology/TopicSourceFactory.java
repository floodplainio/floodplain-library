package com.dexels.navajo.reactive.source.topology;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

import java.util.*;

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
	public Optional<Map<String, String>> parameterTypes() {
		Map<String,String> types = new HashMap<String, String>();
		types.put("name", "string");
		return Optional.of(Collections.unmodifiableMap(types));
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
