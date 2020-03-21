package com.dexels.navajo.reactive.source.input;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.DataItem.Type;
import com.dexels.navajo.reactive.api.ReactiveParameters;
import com.dexels.navajo.reactive.api.ReactiveSource;
import com.dexels.navajo.reactive.api.ReactiveSourceFactory;
import com.dexels.navajo.reactive.api.SourceMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TmlInputSourceFactory implements ReactiveSourceFactory, SourceMetadata {

	@Override
	public ReactiveSource build(ReactiveParameters parameters) {
		return new  TmlInputSource(this, parameters);
	}

	@Override
	public Optional<List<String>> allowedParameters() {
		return Optional.of(Arrays.asList(new String[]{"path"}));
	}

	@Override
	public Optional<List<String>> requiredParameters() {
		return Optional.of(Arrays.asList(new String[]{}));
	}

	@Override
	public Optional<Map<String, ImmutableMessage.ValueType>> parameterTypes() {
		return Optional.of(Map.of("name", ImmutableMessage.ValueType.STRING));
	}


	@Override
	public Type sourceType() {
		return Type.EVENTSTREAM;
	}

	@Override
	public String name() {
		return "tmlinput";
	}
}

