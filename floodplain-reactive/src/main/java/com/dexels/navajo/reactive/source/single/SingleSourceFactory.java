package com.dexels.navajo.reactive.source.single;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;
import com.dexels.navajo.reactive.api.SourceMetadata;

import java.util.*;

public class SingleSourceFactory implements ReactiveSourceFactory, SourceMetadata {

	public SingleSourceFactory() {
	}

//	@Override
//	public ReactiveSource build(String relativePath, String type, List<ReactiveParseProblem> problems, Optional<XMLElement> x, ReactiveParameters params,
//			List<ReactiveTransformer> transformers, Type finalType, Function<String, ReactiveMerger> reducerSupplier
//			) {
//		return new SingleSource(this,params,transformers,finalType,x, relativePath);
//	}


	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new SingleSource(this,parameters);
	}

	@Override
	public Type sourceType() {
		return DataItem.Type.MESSAGE;
	}

	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"count","debug","delay"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Collections.emptyList());
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("count",ImmutableMessage.ValueType.INTEGER,"delay", ImmutableMessage.ValueType.INTEGER,"debug", ImmutableMessage.ValueType.BOOLEAN));
	}

	@Override
	public String name() {
		return "single";
	}

}
