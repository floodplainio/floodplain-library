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


public class DeleteSubMessage implements ReactiveMerger {

	public DeleteSubMessage() {
	}

	@Override
	public Function<StreamScriptContext,Function<DataItem,DataItem>> execute(ReactiveParameters params) {
		return context->(item)->{
			ReactiveResolvedParameters resolved = params.resolve(context, Optional.of(item.message()),item.stateMessage(), this);
			String name = resolved.paramString("name");	
			// both singular and array submessages will be removed
			// TODO this could be more efficient
			boolean condition = resolved.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}

			return DataItem.of(item.message().withoutSubMessages(name).withoutSubMessage(name));
		};
	
	}

	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"name","condition"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{"name"}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("condition",ImmutableMessage.ValueType.BOOLEAN,"name", ImmutableMessage.ValueType.STRING));
	}

	@Override
	public String name() {
		return "deleteSubMessage";
	}

}
