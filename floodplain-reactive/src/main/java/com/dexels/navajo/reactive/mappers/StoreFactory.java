package com.dexels.navajo.reactive.mappers;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;

import java.util.*;

public class StoreFactory implements ReactiveTransformerFactory {

	public StoreFactory() {
	}

	// TODO fix, now broken. Do we need it?

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,
			ReactiveParameters parameters) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"name","value"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"name","value"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("name",ImmutableMessage.ValueType.STRING,"value", ImmutableMessage.ValueType.INTEGER));
	}

	@Override
	public Set<Type> inType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Type outType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String name() {
		return "store";
	}

}
