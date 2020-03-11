package com.dexels.navajo.reactive.source.topology;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;

public class SinkTransformerFactory implements ReactiveTransformerFactory {

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
		return "sink";
	}

	@Override
	public Optional<List<String>> allowedParameters() {
//		return Optional.of(Arrays.asList("logName"));
		return Optional.empty();
	}

	@Override
	public Optional<List<String>> requiredParameters() {
//		return Optional.of(Collections.emptyList());
		return Optional.empty();
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		return Optional.empty();

//		Map<String,String> types = new HashMap<String, String>();
//		types.put("logName", "string");
//		return Optional.of(types);
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems, ReactiveParameters parameters) {
		return new SinkTransformer(this, parameters);
	}

}
