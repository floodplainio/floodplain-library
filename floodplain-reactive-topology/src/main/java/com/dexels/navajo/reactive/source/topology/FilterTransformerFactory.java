package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;
import com.dexels.navajo.reactive.api.TransformerMetadata;

import java.util.*;

public class FilterTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	public FilterTransformerFactory() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems, 
			ReactiveParameters parameters) {

		return new FilterTransformer(this,parameters);
	}


	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {DataItem.Type.MESSAGE,DataItem.Type.SINGLEMESSAGE}));
	}

	@Override
	public Type outType() {
		return DataItem.Type.MESSAGE;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList());
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList());
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("filter",ImmutableMessage.ValueType.BOOLEAN));
	}

	@Override
	public String name() {
		return "filter";
	}
}
