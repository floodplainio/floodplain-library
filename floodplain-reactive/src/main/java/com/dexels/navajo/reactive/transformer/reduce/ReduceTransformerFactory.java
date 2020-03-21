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

public class ReduceTransformerFactory implements ReactiveTransformerFactory, TransformerMetadata {

	public ReduceTransformerFactory() {
	}

	@Override
	public ReactiveTransformer build( List<ReactiveParseProblem> problems,ReactiveParameters parameters) {
			
//		Function<StreamScriptContext,Function<DataItem,DataItem>> reducermapper = ReactiveScriptParser.parseReducerList(relativePath, problems,xml.map(e->(List<XMLElement>)e.getChildren()) , buildContext);
		return new ReduceTransformer(this,parameters);
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"debug"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("debug", ImmutableMessage.ValueType.BOOLEAN));
	}


	@Override
	public Set<Type> inType() {
		return new HashSet<>(Arrays.asList(new Type[] {Type.MESSAGE})) ;
	}

	@Override
	public Type outType() {
		return Type.SINGLEMESSAGE;
	}
	

	@Override
	public String name() {
		return "reduce";
	}
}
