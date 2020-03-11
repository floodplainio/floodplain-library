package com.dexels.navajo.functions;

import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

public class ExistsProperty extends StatefulFunctionInterface {

	@Override
	public Object evaluate() throws TMLExpressionException {
		if (getOperands().size() != 1) {
			throw new TMLExpressionException(this, "Invalid function call");
		}
		Object o = getOperand(0);
		if ( !(o instanceof String) ) {
			throw new TMLExpressionException(this, "Invalid function call");
		}
		Navajo in = getNavajo();
		return ( in.getProperty((String) o) != null );
		
	}

	@Override
	public String remarks() {
		return "Checks whether a property exists in the Input Navajo";
	}

	@Override
	public String usage() {
		return "ExistsProperty(fully qualified property name)";
	}

}