package com.dexels.navajo.reactive.source.topology;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveTransformer;
import com.dexels.navajo.reactive.api.ReactiveTransformerFactory;

import java.util.*;

public class LogTransformerFactory implements ReactiveTransformerFactory {

	@Override
	public Set<Type> inType() {
        return new HashSet<>(Arrays.asList(new Type[] {DataItem.Type.MESSAGE}));
	}

	@Override
	public Type outType() {
		return Type.MESSAGE;
	}

	@Override
	public String name() {
		return "log";
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[] {"every","logName","dumpStack"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[] {}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("every",ImmutableMessage.ValueType.INTEGER,"dumpStack",ImmutableMessage.ValueType.BOOLEAN,"logName", ImmutableMessage.ValueType.STRING));
	}

	@Override
	public ReactiveTransformer build(List<ReactiveParseProblem> problems,
			ReactiveParameters parameters) {
		return new LogTransformer(this,parameters);
	}

}
