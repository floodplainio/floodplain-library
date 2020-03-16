package com.dexels.navajo.reactive.api;

import com.dexels.navajo.document.stream.DataItem.Type;

import java.util.*;

public class StandardTransformerMetadata {

	public StandardTransformerMetadata() {
	}

	
	public static TransformerMetadata noParams(Type inType, Type outType) {
		return noParams(new HashSet<>(Arrays.asList(new Type[] {inType})), outType);
	}
	
	public static TransformerMetadata noParams(Set<Type> inType, Type outType) {
		return new TransformerMetadata() {
			
			@Override
			public Set<Type> inType() {
				return inType;
			}

			@Override
			public Type outType() {
				return outType;
			}

			@Override
			public Optional<List<String>> allowedParameters() {
				return Optional.of(Collections.emptyList());
			}

			@Override
			public Optional<List<String>> requiredParameters() {
				return Optional.of(Collections.emptyList());
			}

			@Override
			public Optional<Map<String, String>> parameterTypes() {
				return Optional.of(Collections.emptyMap());
			}

			@Override
			public String name() {
				return "identity";
			}
		};
		
	}
}
