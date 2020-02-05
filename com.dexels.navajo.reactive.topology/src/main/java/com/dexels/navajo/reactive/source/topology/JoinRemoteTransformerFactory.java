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
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;

public class JoinRemoteTransformerFactory implements ReactiveTransformerFactory {

	@Override
	public Set<Type> inType() {
		Set<Type> types = new HashSet<>();
		types.add( Type.MESSAGE);
		return Collections.unmodifiableSet(types);
	}

	// TODO perhaps introduce type 'NONE'? or 'TERMINAL'?
	@Override
	public Type outType() {
		return Type.MESSAGE;
	}

	@Override
	public String name() {
		return "joinRemote";
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"key"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {}));
}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String,String> types = new HashMap<>();
		return Optional.of(Collections.unmodifiableMap(types));
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems, ReactiveParameters parameters) {
		return new JoinRemoteTransformer(this, parameters);
	}

}