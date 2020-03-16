package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.io.StringWriter;

public class GetRequest extends StatefulFunctionInterface {

	@Override
	public String remarks() {
		return "Returns the current request as a string";
	}

	@Override
	public Object evaluate() throws TMLExpressionException {
		StringWriter sb = new StringWriter();
		inMessage.write(sb);
		return sb.toString();
	}

}
