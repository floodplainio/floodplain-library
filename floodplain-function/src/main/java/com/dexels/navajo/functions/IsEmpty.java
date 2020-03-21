package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.util.List;

public class IsEmpty extends FunctionInterface {

	@Override
	public Object evaluate() throws TMLExpressionException {

		Object arg = this.getOperands().get(0);
		
		if (arg == null) {
			return Boolean.TRUE;
		}

		if ( arg instanceof String) {
			return (((String) arg).trim().equals(""));
		}
		
		if ( arg instanceof List ) {
			return (((List<?>) arg).size() == 0);
		}
		
		return Boolean.FALSE;

	}

	@Override
	public String remarks() {
		return "Determines whether a given Navajo Object is empty or null";
	}

}
