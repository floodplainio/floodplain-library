package com.dexels.navajo.functions;

import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.document.types.Binary;
import com.dexels.navajo.document.types.BinaryDigest;
import com.dexels.navajo.expression.api.FunctionInterface;

import java.io.StringWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Jarno Posthumus
 */
public class MD5Sum extends FunctionInterface {

	public MD5Sum() {
	}

	@Override
	public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {
		String output = "unknown";
		if (getOperand(0) == null) {
			return Integer.valueOf(0);
		}
		if ( getOperand(0) instanceof Binary ) {
			Binary binaryFile = (Binary) getOperand(0);	
			return binaryFile.getHexDigest();
		}
		
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		
		if ( getOperand(0) instanceof Message ) { 
			Message m = (Message) getOperand(0);
			StringWriter sw = new StringWriter();
			m.write(sw);
			md5.update(sw.toString().getBytes());
		} else {
			md5.update((getOperand(0)+"").getBytes());	
		}
		
		byte[] array = md5.digest();
		if (getOperands().size() > 1 && getOperand(1) instanceof Boolean && (boolean) getOperand(1)) {
			// return hex representation
			return new BinaryDigest(array).hex();
		}
		BigInteger bigInt = new BigInteger(1, array);
		output = bigInt.toString(16);
		
		return output;

	}

	@Override
	public String remarks() {
		return "Get the MD5Sum of supplied Binary object.";
	}


}
