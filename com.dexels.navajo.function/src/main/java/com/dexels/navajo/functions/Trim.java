package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2002
 * </p>
 * <p>
 * Company: Dexels.com
 * </p>
 * 
 * @author unascribed
 * @version $Id: e093f55832744e22a1ecf565fc72108b5e4d58e2 $
 */

public final class Trim extends FunctionInterface {

	public static final String vcIdent = "$Id: e093f55832744e22a1ecf565fc72108b5e4d58e2 $";

	public Trim() {
	}
    @Override
	public boolean isPure() {
    		return false;
    }

	@Override
	public final Object evaluate()
			throws com.dexels.navajo.expression.api.TMLExpressionException {

		final Object op = this.getOperands().get(0);

		if (op == null) {
			return ("");
		}

		if (!(op instanceof java.lang.String)) {
			throw new TMLExpressionException(this, "String argument expected");
		}

		final String s = (String) op;

		return s.trim();
	}

	@Override
	public String usage() {
		return "Trim(string)";
	}

	@Override
	public String remarks() {
		return "Trims a string";
	}

	public static void main(String[] args) throws TMLExpressionException {
		Trim t = new Trim();
		t.reset();
		t.insertStringOperand("   aaap     ");
		String res = (String) t.evaluate();
		System.err.println(">" + res + "<");
	}
} // public class Trim extends FunctionInterface

// EOF: $RCSfile$ //