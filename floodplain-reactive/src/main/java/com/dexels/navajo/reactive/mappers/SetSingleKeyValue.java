package com.dexels.navajo.reactive.mappers;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.operand.Operand;
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


public class SetSingleKeyValue implements ReactiveMerger {

	@Override
	public Function<StreamScriptContext, Function<DataItem, DataItem>> execute(ReactiveParameters params) {
		return context->item->{
			// TODO need to accept @param= key values
			ImmutableMessage s = item.message();
			ReactiveResolvedParameters parms = params.resolve(context, Optional.of(s),item.stateMessage(), this);
			boolean condition = parms.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}
			String toValue = parms.paramString("to");
			Operand value = parms.namedParameters().get("value");
			return DataItem.of(item.message().with(toValue, value.value, value.type));
		};
	
	}

	
	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList("to","value","condition"));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList("to","value"));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("condition", ImmutableMessage.ValueType.BOOLEAN,"to", ImmutableMessage.ValueType.STRING));
	}

	@Override
	public String name() {
		return "singleKeyValue";
	}

}
