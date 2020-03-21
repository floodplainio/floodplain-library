package com.dexels.navajo.functions.test;

import com.dexels.navajo.document.*;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.functions.util.FunctionFactoryFactory;
import com.dexels.navajo.parser.Expression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("unused")

public class Switch extends AbstractFunction {


	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
		fff = FunctionFactoryFactory.getInstance();
		cl = getClass().getClassLoader();
	}

	@After
	public void tearDown() throws Exception {
	}


	@Test
	public void testSingleSimpleHit() throws Exception {
		FunctionInterface fi = fff.getInstance(cl, "Switch");
		fi.reset();

		fi.insertIntegerOperand(10);
		fi.insertIntegerOperand(10);
		fi.insertIntegerOperand(-10);
		fi.insertIntegerOperand(0);
		Object result = fi.evaluate();
		assertNotNull(result);
		assertEquals(result.getClass(), Integer.class);
		assertEquals(Integer.valueOf(-10), result);

	}

	@Test
	public void testSingleSimpleMiss() throws Exception {
		FunctionInterface fi = fff.getInstance(cl, "Switch");
		fi.reset();

		fi.insertIntegerOperand((Integer) Expression.evaluate("20").value);
		fi.insertIntegerOperand((Integer) Expression.evaluate("10").value);
		fi.insertIntegerOperand((Integer) Expression.evaluate("-10").value);
		fi.insertIntegerOperand((Integer) Expression.evaluate("0").value);
		Object result = fi.evaluate();
		assertNotNull(result);
		assertEquals(result.getClass(), Integer.class);
		assertEquals(Integer.valueOf(0), result);

	}




	@Test
	public void testUnicodeExpressionFunction() throws Exception {
		Operand result = Expression.evaluate("Unicode('0x20AC')");
		System.err.println("Result:"+result.value);
	}	
	
	@Test
	public void testDirectUnicodeExpressionFunction() throws Exception {
		Operand result = Expression.evaluate("'€2,29'");
		System.err.println("Result:"+result.value);
	}	
	
}
