package com.dexels.navajo.functions;

import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoException;
import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.io.StringWriter;

public class NavajoRequestToString extends StatefulFunctionInterface {

	@Override
	public Object evaluate() throws TMLExpressionException {
		Navajo in = getNavajo().copy();
		in.removeHeader();
		in.removeInternalMessages();
		StringWriter ws = new StringWriter();
		try {
			in.write(ws);
		} catch (NavajoException e) {
			throw new TMLExpressionException(this, e.getMessage(), e);
		}
		return ws.toString();
	}

	@Override
	public String remarks() {
		return "Serializes a Navajo request to a string";
	}
	

}
