package com.dexels.navajo.reactive.transformer.mergestream;

import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.*;

import java.util.*;

public class FlatMapTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	
	private ReactiveSource childSource = null;

	public FlatMapTransformerFactory() {
	}
	
	public void activate() {
	}
	
	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,
			ReactiveParameters parameters) {
		return new FlatMapTransformer(this,parameters);
	}

	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {Type.MESSAGE,Type.SINGLEMESSAGE})) ;
	}


	@Override
	public Type outType() {
		return childSource.sourceType();
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"parallel"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Collections.emptyList());
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String, String> r = new HashMap<>();
		r.put("parallel", "integer");
		return Optional.of(Collections.unmodifiableMap(r));

	}

	@Override
	public String name() {
		return "flatmap";
	}

}
