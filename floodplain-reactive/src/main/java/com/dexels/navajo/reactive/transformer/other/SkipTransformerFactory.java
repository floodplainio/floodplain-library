package com.dexels.navajo.reactive.transformer.other;

import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;
import com.dexels.navajo.reactive.api.TransformerMetadata;

import java.util.*;

public class SkipTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	public SkipTransformerFactory() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,
			ReactiveParameters parameters) {
		return new SkipTransformer(this,parameters);
	}
	
	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {DataItem.Type.MESSAGE}));
	}

	@Override
	public Type outType() {
		return DataItem.Type.MESSAGE;
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"count"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {"count"}));
	}

	@Override
	public Optional<Map<String, String>> parameterTypes() {
		Map<String, String> r = new HashMap<>();
		r.put("count", Property.INTEGER_PROPERTY);
		return Optional.of(Collections.unmodifiableMap(r));
	}

	@Override
	public String name() {
		return "skip";
	}

}
