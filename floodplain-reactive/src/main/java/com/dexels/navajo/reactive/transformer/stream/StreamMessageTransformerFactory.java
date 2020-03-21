package com.dexels.navajo.reactive.transformer.stream;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;
import com.dexels.navajo.reactive.api.TransformerMetadata;

import java.util.*;

public class StreamMessageTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	public StreamMessageTransformerFactory() {
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems, ReactiveParameters parameters) {

		return new StreamMessageTransformer(this,parameters);
	}


	@Override
	public Set<Type> inType() {
		return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(new Type[]{Type.SINGLEMESSAGE,Type.MESSAGE}))); // Type.SINGLEMESSAGE;
	}

	@Override
	public Type outType() {
		return Type.EVENT;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"messageName","isArray"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"messageName"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("messageName", ImmutableMessage.ValueType.STRING,"isArray", ImmutableMessage.ValueType.BOOLEAN));
	}
	

	@Override
	public String name() {
		return "tml";
	}

}
