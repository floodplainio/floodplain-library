package com.dexels.navajo.functions.test;

import com.dexels.config.runtime.RuntimeConfig;
import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.FunctionClassification;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.parser.compiled.api.CacheSubexpression;
import com.dexels.navajo.parser.compiled.api.ExpressionCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class TestCompiledExpression {

//	private Navajo input;
	
	

	@Before
	public void setup() {

	}

	@Test
	public void parseFunction() throws TMLExpressionException {

		Operand o = ExpressionCache.getInstance().evaluate("ToUpper('ble')",Optional.<ImmutableMessage>empty(),Optional.<ImmutableMessage>empty());
		System.err.println(": "+o);
		Assert.assertEquals("BLE", o.value);
	}
	
	@Test
	public void testFunctionParamTypeError() throws TMLExpressionException {
		List<String> problems = new ArrayList<>();
		ContextExpression o = ExpressionCache.getInstance().parse(problems,"ToUpper(1)",name->FunctionClassification.DEFAULT);
		System.err.println("problems: "+problems);
		System.err.println("returntype: "+o.returnType());
		System.err.println("immutable: "+o.isLiteral());
		if(RuntimeConfig.STRICT_TYPECHECK.getValue()!=null) {
			Assert.assertEquals(1, problems.size());
		} else {
			Assert.assertEquals("Don't expect problems to appear when STRICT_TYPECHECK is false", 0,problems.size());
//			Assert.fail("Failed test, ony valid when STRICT_TYPECHECK. Set env for unit tests.");
		}
		
	}
	
	@Test
	public void testNestedFunctionType() throws TMLExpressionException {
		List<String> problems = new ArrayList<>();
		ContextExpression o = ExpressionCache.getInstance().parse(problems,"ToUpper(ToLower('Bob'))",name->FunctionClassification.DEFAULT);
		System.err.println("problems: "+problems);
		System.err.println("returntype: "+o.returnType().orElse("<unknown>"));
		Assert.assertTrue("Expected a return type here", o.returnType().isPresent());
		Assert.assertEquals("string", o.returnType().get());
		System.err.println("immutable: "+o.isLiteral());
	}
	
	@Test
	public void testImmutablilityPropagation() throws TMLExpressionException {
		List<String> problems = new ArrayList<>();
		ContextExpression o = ExpressionCache.getInstance().parse(problems,"ToUpper(ToLower('Bob'))",name->FunctionClassification.DEFAULT);
		System.err.println("problems: "+problems);
		System.err.println("immutable: "+o.isLiteral());
		Assert.assertTrue(o.isLiteral());
	}
	@Test
	public void testImmutablilityPropagationPerformance() throws TMLExpressionException {
		CacheSubexpression.setCacheSubExpression(true);;
		List<String> problems = new ArrayList<>();
		ContextExpression o = ExpressionCache.getInstance().parse(problems,"ToUpper(ToLower('Bob'))",name->FunctionClassification.DEFAULT);
		long now = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			o.apply();
//			.value;
//			System.err.println("tr:" +tr);
		}
		System.err.println("Now: "+(System.currentTimeMillis()-now));
		System.err.println("problems: "+problems);
		System.err.println("immutable: "+o.isLiteral());
		Assert.assertTrue(o.isLiteral());
	}
		
	@Test
	public void testFunctionType() {
		ExpressionCache ce = ExpressionCache.getInstance();
		List<String> problems = new ArrayList<>();
		ContextExpression cx = ce.parse(problems,"ToUpper([whatever])",name->FunctionClassification.DEFAULT);
		Assert.assertEquals(Property.STRING_PROPERTY, cx.returnType().get());
	}
	
	@Test
	public void testFunctionEvaluation() {
		List<String> problems = new ArrayList<>();
		ExpressionCache ce = ExpressionCache.getInstance();
		String embeddedExpression = "StringFunction('matches','aaa', '.*[-]+7[0-9][A-z]*$' ))";
		ContextExpression cemb = ce.parse(problems,embeddedExpression,name->FunctionClassification.DEFAULT);
		System.err.println("Problems: "+problems);
		Assert.assertEquals(0, problems.size());
		Operand result =  cemb.apply(Optional.empty(),Optional.empty());
		System.err.println("Result: "+result.type+" value: "+result.value);
		
	}


}
