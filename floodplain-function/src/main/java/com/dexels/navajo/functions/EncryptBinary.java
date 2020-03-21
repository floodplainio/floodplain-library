package com.dexels.navajo.functions;

import com.dexels.navajo.document.operand.Binary;
import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.functions.security.Security;

public class EncryptBinary extends FunctionInterface {

	@Override
	public String remarks() {
		return "Encrypts a binary using a 128 bit key";
	}

	@Override
	public Object evaluate() throws TMLExpressionException {
		
		String result = null;
		
		String key = (String) getOperand(0);
		Binary image = (Binary) getOperand(1);
		
		try {
			Security s = new Security(key);
			result = s.encrypt(image).replace("\n", "");
		} catch (Exception e) {
			throw new TMLExpressionException(e.getMessage());
		}
		
		return result;
	}

}
