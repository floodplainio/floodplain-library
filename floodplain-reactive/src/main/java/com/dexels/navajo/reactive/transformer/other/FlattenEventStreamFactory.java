package com.dexels.navajo.reactive.transformer.other;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;
import com.dexels.navajo.reactive.api.TransformerMetadata;

import java.util.*;

public class FlattenEventStreamFactory implements ReactiveTransformerFactory, TransformerMetadata {

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,
			ReactiveParameters parameters) {
		return new FlattenEventStream(this,parameters);
	}
	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {DataItem.Type.EVENTSTREAM}));
	}

	@Override
	public Type outType() {
		return DataItem.Type.EVENT;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"parallel"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("parallel", ImmutableMessage.ValueType.INTEGER));
	}
	

	@Override
	public String name() {
		return "flatten";
	}

}
