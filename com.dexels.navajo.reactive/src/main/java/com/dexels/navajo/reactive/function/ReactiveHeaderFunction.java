package com.dexels.navajo.reactive.function;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

public class ReactiveHeaderFunction extends FunctionInterface {

	@Override
	public String remarks() {
		return "Reactive Headers";
	}

	@Override
	public Object evaluate() throws TMLExpressionException {
		String mime = (String) super.getNamedParameters().get("mimeType").value;
		
		
		return null;
	}

}