package com.dexels.navajo.functions.test;

import com.dexels.navajo.document.*;
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

	@Override
	protected Navajo createTestNavajo() throws Exception {
		Navajo doc = NavajoFactory.getInstance().createNavajo();
		Message array = NavajoFactory.getInstance().createMessage(doc, "Aap");
		array.setType(Message.MSG_TYPE_ARRAY);
		Message array1 = NavajoFactory.getInstance().createMessage(doc, "Aap");
		array.addElement(array1);
		doc.addMessage(array);
		Property p = NavajoFactory.getInstance().createProperty(doc, "Noot", Property.INTEGER_PROPERTY, "10", 10, "", "in");
		p.setValue(10);
		array1.addProperty(p);


		Message single = NavajoFactory.getInstance().createMessage(doc, "Single");
		doc.addMessage(single);
		Property p2 = NavajoFactory.getInstance().createProperty(doc, "Selectie", "1", "", "in");
		p2.addSelection(NavajoFactory.getInstance().createSelection(doc, "key", "value", true));
		single.addProperty(p2);
		Property p3 = NavajoFactory.getInstance().createProperty(doc, "Vuur", Property.INTEGER_PROPERTY, "10", 10, "", "out");
		p3.setValue(10);
		single.addProperty(p3);

		return doc;
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
	public void testWithNotZeroParameters() throws Exception {
		FunctionInterface fi = fff.getInstance(cl, "Switch");
		fi.reset();
		Navajo n = createTestNavajo();
//		fi.setInMessage(n);


		try {
			Object result = fi.evaluate();
			} catch (TMLExpressionException tmle) {
				assertTrue(tmle.getMessage().indexOf("Not enough") != -1);
			}

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
