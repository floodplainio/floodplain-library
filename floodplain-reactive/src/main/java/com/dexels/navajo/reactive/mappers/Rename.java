package com.dexels.navajo.reactive.mappers;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.reactive.api.ReactiveMerger;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveResolvedParameters;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.dexels.immutable.api.ImmutableMessage.ValueType;


public class Rename implements ReactiveMerger {

	public Rename() {
	}

	@Override
	public Function<StreamScriptContext,Function<DataItem,DataItem>> execute(ReactiveParameters params) {
		return context->(item)->{
			ReactiveResolvedParameters parms = params.resolve(context, Optional.of(item.message()), item.stateMessage(), this);
			boolean condition = parms.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}
			String fromKey = parms.paramString("from");
			Object oldValue = item.message().value(fromKey).orElse(null);
			ValueType oldType = item.message().columnType(fromKey);
			
			return DataItem.of(item.message().without(fromKey ).with(parms.paramString("to"),oldValue, oldType));
		};
	
	}
	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"to","from","condition"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"to","from"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("condition", ImmutableMessage.ValueType.BOOLEAN,"to",ValueType.STRING,"from",ValueType.STRING));
	}

	@Override
	public String name() {
		return "rename";
	}

}
