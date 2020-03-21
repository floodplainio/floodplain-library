package com.dexels.navajo.reactive.api;

import com.dexels.immutable.api.ImmutableMessage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ParameterValidator {
	public Optional<List<String>> allowedParameters();
	public Optional<List<String>> requiredParameters();
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes();
	public String name();

}
