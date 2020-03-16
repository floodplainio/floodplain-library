package com.dexels.navajo.functions;

import com.dexels.navajo.document.*;
import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Title: Navajo Product Project
 * </p>
 * <p>
 * Description: This is the official source for the Navajo server
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Dexels BV
 * </p>
 * 
 * @author Arjen Schoneveld
 * @version 1.0
 */

public class ArraySelection extends StatefulFunctionInterface {

	

	@Override
	public String remarks() {
		// 'MessagePath' 'NameProperty' 'ValueProperty'
		return "Checks whether properties in an array message have unique values";
	}

	@Override
	public Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {

		if (getOperands().size() < 3) {
			throw new TMLExpressionException(this, usage());
		}

		String messageName = getStringOperand(0);
		String propertyName = getStringOperand(1);
		String valuePropertyName = getStringOperand(2);

//		Message parent = getCurrentMessage();
		Navajo doc = getNavajo();
		Message array = doc.getMessage(messageName);
		if (array == null) {
			throw new TMLExpressionException(this, "Empty or non existing array message: " + messageName);
		}

		List<Message> arrayMsg = array.getAllMessages();

		List<Selection> result = new ArrayList<Selection>();
		if (arrayMsg == null) {
			throw new TMLExpressionException(this, "Empty or non existing array message: " + messageName);
		}
		for (int i = 0; i < arrayMsg.size(); i++) {
			Message m = arrayMsg.get(i);
			Property p = m.getProperty(propertyName);
			Property val = m.getProperty(valuePropertyName);
			Selection s = NavajoFactory.getInstance().createSelection(doc, "" + p.getTypedValue(), "" + val.getTypedValue(), false);
			result.add(s);
		}
		return result;
	}

	@Override
	public String usage() {
		return "ArraySelection(<Array message name (absolute path)>,<Name Property name>,<Value Property name>)";
	}

}