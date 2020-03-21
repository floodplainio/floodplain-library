package com.dexels.navajo.reactive.transformer.reduce;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;
import com.dexels.navajo.reactive.api.TransformerMetadata;

import java.util.*;

public class ReduceToListTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	public ReduceToListTransformerFactory() {
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,ReactiveParameters parameters) {
		return new ReduceToListTransformer(this,parameters);
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"name","debug"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {"name"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("debug", ImmutableMessage.ValueType.BOOLEAN,"name", ImmutableMessage.ValueType.STRING));
	}


	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {Type.MESSAGE})) ;
	}

	@Override
	public Type outType() {
		return Type.MSGLIST;
	}
	

	@Override
	public String name() {
		return "reducetolist";
	}
}
