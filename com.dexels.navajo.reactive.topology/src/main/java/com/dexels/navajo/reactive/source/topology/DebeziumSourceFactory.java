package com.dexels.navajo.reactive.source.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;

public class DebeziumSourceFactory implements ReactiveSourceFactory {

	@Override
	public Type sourceType() {
		return Type.MESSAGE;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList("topic","appendTenant","appendSchema"));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList("topic"));
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String, String> r = new HashMap<>();
		r.put("topic", Property.STRING_PROPERTY);
		r.put("appendTenant", Property.BOOLEAN_PROPERTY);
		r.put("appendSchema", Property.BOOLEAN_PROPERTY);
		return Optional.of(Collections.unmodifiableMap(r));
	}


	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new DebeziumSource(this,parameters);
	}

	@Override
	public String sourceName() {
		return "debezium";
	}



}
