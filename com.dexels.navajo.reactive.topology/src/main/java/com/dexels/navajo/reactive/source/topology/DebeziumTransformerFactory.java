package com.dexels.navajo.reactive.source.topology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;

public class DebeziumTransformerFactory implements ReactiveTransformerFactory {

	@Override
	public Set<Type> inType() {
		Set<Type> types = new HashSet<>();
		types.add( Type.MESSAGE);
		return Collections.unmodifiableSet(types);
	}

	@Override
	public Type outType() {
		return Type.MESSAGE;
	}

	@Override
	public String name() {
		return "debezium";
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
	public ReactiveTransformer build(List<ReactiveParseProblem> problems, ReactiveParameters parameters) {
		return new DebeziumTransformer(this, parameters);
	}

}
