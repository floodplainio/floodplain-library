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

public class StoreSingle implements ReactiveMerger {

	public StoreSingle() {
	}

	@Override
	public Function<StreamScriptContext,Function<DataItem,DataItem>> execute(ReactiveParameters params) {
		return context->item->{
			// will use the second message as input, if not present, will use the source message
			ImmutableMessage s = item.message();
			ImmutableMessage state = item.stateMessage();
			ReactiveResolvedParameters parms = params.resolve(context, Optional.of(s),state , this);
			boolean condition = parms.optionalBoolean("condition").orElse(true);
			if(!condition) {
				return item;
			}			
			Operand resolvedValue = parms.namedParameters().get("value");
			String toValue = parms.paramString("to");
			ImmutableMessage di = item.stateMessage().with(toValue, resolvedValue.value,resolvedValue.type);

			return DataItem.of(s,di);
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
		return Optional.of(Map.of("condition",ImmutableMessage.ValueType.BOOLEAN,"to", ImmutableMessage.ValueType.STRING));
	}

	@Override
	public String name() {
		return "storeSingle";
	}

}
