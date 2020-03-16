package com.dexels.navajo.reactive.api;

import com.dexels.navajo.document.stream.DataItem;
import com.dexels.navajo.document.stream.DataItem.Type;

import java.util.Set;

public interface TransformerMetadata extends ParameterValidator {

	public Set<Type> inType();
	public DataItem.Type outType();
}
