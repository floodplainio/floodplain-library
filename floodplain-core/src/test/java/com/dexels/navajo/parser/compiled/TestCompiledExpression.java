package com.dexels.navajo.parser.compiled;

import com.dexels.navajo.document.*;
import com.dexels.navajo.expression.api.*;
import com.dexels.navajo.expression.compiled.AddTestFunction;
import com.dexels.navajo.expression.compiled.ParameterNamesFunction;
import com.dexels.navajo.functions.util.FunctionFactoryFactory;
import com.dexels.navajo.parser.Expression;
import com.dexels.navajo.parser.NamedExpression;
import com.dexels.navajo.parser.compiled.api.ExpressionCache;
import com.dexels.navajo.script.api.SystemException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class TestCompiledExpression {

	
	private Navajo input;


	@Before
	public void setup() {
		input = NavajoFactory.getInstance().createNavajo();
		input.addMessage(NavajoFactory.getInstance().createMessage(input,"TestMessage")).addProperty(NavajoFactory.getInstance().createProperty(input, "TestProperty", Property.STRING_PROPERTY, "TestValue", 99, "TestDescription", Property.DIR_OUT));
		Message createMessage = NavajoFactory.getInstance().createMessage(input,"TestArrayMessageMessage",Message.MSG_TYPE_ARRAY);
		input.addMessage(createMessage);
		Message element1 = NavajoFactory.getInstance().createMessage(input, "TestArrayMessageMessage", Message.MSG_TYPE_ARRAY_ELEMENT);
		element1.addProperty(NavajoFactory.getInstance().createProperty(input,"Property",Property.STRING_PROPERTY,"Prop",99,"",Property.DIR_IN));
		Message element2 = NavajoFactory.getInstance().createMessage(input, "TestArrayMessageMessage", Message.MSG_TYPE_ARRAY_ELEMENT);
		Message params = NavajoFactory.getInstance().createMessage(input, "__parms__");
		params.addProperty(NavajoFactory.getInstance().createProperty(input,"Param",Property.STRING_PROPERTY,"SomeParam",99,"",Property.DIR_IN));
		input.addMessage(params);
		element2.addProperty(NavajoFactory.getInstance().createProperty(input,"Property",Property.STRING_PROPERTY,"Prop2",99,"",Property.DIR_IN));
		createMessage.addElement(element1);
		createMessage.addElement(element2);
	}
	@Test
	public void parseIntAddition() throws ParseException, TMLExpressionException {
		List<String> problems = new ArrayList<>();
        String expression = "1+1";
		ContextExpression ss =  ExpressionCache.getInstance().parse(problems,expression,fn->FunctionClassification.DEFAULT);
        ContextExpression ss2 =  ExpressionCache.getInstance().parse(problems,expression,fn->FunctionClassification.DEFAULT);
        if(!problems.isEmpty()) {
        		throw new TMLExpressionException(problems,expression);
        }
        System.err.println("ss: "+ss.isLiteral());
        System.err.println("ss2: "+ss2.isLiteral());
        System.err.println("Result: "+ss.apply().value);
        Assert.assertEquals(2, ss.apply().value);
        Assert.assertEquals(2, ss2.apply().value);
        Assert.assertTrue(ss.isLiteral());
        Assert.assertTrue(ss2.isLiteral());

	}




	@Test @Ignore
	public void testParseTmlConditionalComplex() throws ParseException, TMLExpressionException {
		String expression = "{request@.:!?[/NewMemberFunction/FromUnion]} AND {response@.:!?[/ExistingClubFunction/PersonId]}";
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		cp.Expression();
		List<String> problems = new ArrayList<>();
        ContextExpression ss = cp.getJJTree().rootNode().interpretToLambda(problems,sr.toString(),fn->FunctionClassification.DEFAULT,name->Optional.empty());
        if(!problems.isEmpty()) {
    			throw new TMLExpressionException(problems,expression);
        }
        ss.apply();
	}

	
	


	

	@Test
	public void testMultilineStringLiteral() throws ParseException, TMLExpressionException, SystemException {
		String clause = "'what is a haiku\n" + 
				"nothing but words, poetic?\n" + 
				"this is a haiku'";
		String o = (String) Expression.evaluate(clause).value;
		int lines = o.split("\n").length;
		Assert.assertEquals(3, lines);
	}



	@Test
	public void testNamedExpression() throws ParseException {
		String expression = "aap=1+1";
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		cp.KeyValue();		
		ASTKeyValueNode atn = (ASTKeyValueNode) cp.getJJTree().rootNode();
		List<String> problems = new ArrayList<>();
		NamedExpression ne = (NamedExpression) atn.interpretToLambda(problems, expression,fn->FunctionClassification.DEFAULT,name->Optional.empty());
		System.err.println("Problems: "+problems);
		Assert.assertEquals(0, problems.size());
		Assert.assertEquals("aap",ne.name);
		Assert.assertEquals(2, ne.apply().value);
	}

	@Test
	public void testFunctionCallWithNamedParams() throws ParseException {
        FunctionInterface testFunction = new AddTestFunction();
        FunctionDefinition fd = new FunctionDefinition(testFunction.getClass().getName(), "blib", "bleb", "blab");
        FunctionFactoryFactory.getInstance().addExplicitFunctionDefinition("addtest",fd);
		String expression = "addtest(aap='blub',3+5,4)";
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		cp.Expression();
		SimpleNode atn = (SimpleNode) cp.getJJTree().rootNode();
		List<String> problems = new ArrayList<>();
		ContextExpression ne = atn.interpretToLambda(problems, expression,fn->FunctionClassification.DEFAULT,name->Optional.empty());
		Operand result = ne.apply();
		System.err.println("Final: "+result.value);
		Assert.assertEquals("monkey", result.value);
	}

	@Test
	public void testEmptyFunctionCall() throws ParseException {
       FunctionInterface testFunction = new AddTestFunction();
        FunctionDefinition fd = new FunctionDefinition(testFunction.getClass().getName(), "blib", "bleb", "blab");
        FunctionFactoryFactory.getInstance().addExplicitFunctionDefinition("addtest",fd);

		String expression = "addtest()";
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		cp.Expression();
		List<String> problems = new ArrayList<>();
        ContextExpression ss = cp.getJJTree().rootNode().interpretToLambda(problems,sr.toString(),fn->FunctionClassification.DEFAULT,name->Optional.empty());
        Operand o = ss.apply();
		Assert.assertEquals("monkey", o.value);
        System.err.println(">> "+o);
	}
	
	@Test
	public void testMultiArgFunction() throws Exception {
        FunctionInterface testFunction = new AddTestFunction();
        FunctionDefinition fd = new FunctionDefinition(testFunction.getClass().getName(), "blib", "bleb", "blab");
        FunctionFactoryFactory.getInstance().addExplicitFunctionDefinition("SingleValueQuery",fd);
		String expression = 	"SingleValueQuery( 'aap','noot' )";
		
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		List<String> problems = new ArrayList<>();
		cp.Expression();
        ContextExpression ss = cp.getJJTree().rootNode().interpretToLambda(problems,sr.toString(),fn->FunctionClassification.DEFAULT,name->Optional.empty());
        System.err.println("ss: "+ss.getClass());
	}


	@Test
	public void testNestedNamedFunction() throws Exception {
        FunctionInterface testFunction = new AddTestFunction();
        FunctionDefinition fd = new FunctionDefinition(testFunction.getClass().getName(), "description", "input", "result");
        FunctionFactoryFactory.getInstance().addExplicitFunctionDefinition("MysteryFunction",fd);
        
		String expression = 	"MysteryFunction(eep=MysteryFunction('blib','blob'), 'aap','noot' )";
		
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		List<String> problems = new ArrayList<>();
		cp.Expression();
        ContextExpression ss = cp.getJJTree().rootNode().interpretToLambda(problems,sr.toString(),fn->FunctionClassification.DEFAULT,name->Optional.empty());
        System.err.println("ss: "+ss.apply().getClass());
	}
	
	@Test
	public void testNestedNamedParams() throws Exception {
        FunctionInterface testFunction = new ParameterNamesFunction();
        FunctionDefinition fd = new FunctionDefinition(testFunction.getClass().getName(), "description", "input", "result");
        FunctionFactoryFactory.getInstance().addExplicitFunctionDefinition("ParameterNamesFunction",fd);
        
		String expression = 	"ParameterNamesFunction(aap=1+1,noot=2+2)";
		
		StringReader sr = new StringReader(expression);
		CompiledParser cp = new CompiledParser(sr);
		List<String> problems = new ArrayList<>();
		cp.Expression();
        ContextExpression ss = cp.getJJTree().rootNode().interpretToLambda(problems,sr.toString(),fn->FunctionClassification.DEFAULT,name->Optional.empty());
        System.err.println("ss: "+ss.apply().value);
        Assert.assertEquals("aap,noot", ss.apply().value);
	}
	
	//	Unicode(hex-string)
	@Test
	public void testUnicodeExpression() throws Exception {
		Operand result = Expression.evaluate("'耀'", null,null,null);
		System.err.println("Result:"+result.value);
	}	
	@Test
	public void testUnicodeExpressionEscaped() throws Exception {
		Operand result = Expression.evaluate("'\u20AC2,29'", null,null,null);
		System.err.println("Result:"+result.value);
	}	
	
	@Test
	public void testDoubleComparison()
	{
		Operand result; 
		result = Expression.evaluate(" 1.0 < 1.1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1.0 < 1.0 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 <= 1.1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1.0 <= 1.0 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1.0 > 1.1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 > 1.0 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 >= 1.1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 >= 1.0 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}
	
	@Test
	public void testDoubleIntegerComparison()
	{
		Operand result; 
		result = Expression.evaluate(" 0.9 < 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1.0 < 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 0.9 <= 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1.0 <= 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 0.9 > 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 > 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 0.9 >= 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1.0 >= 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}
	
	@Test
	public void testIntegerDoubleComparison()
	{
		Operand result; 
		result = Expression.evaluate(" 1 < 1.1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 < 1.0 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 <= 1.1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 <= 1.0 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 > 1.1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 > 1.0 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 >= 1.1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 >= 1.0 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}
	
	@Test
	public void testIntegerComparison()
	{
		Operand result; 
		result = Expression.evaluate(" 1 < 2 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 < 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 <= 2 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 <= 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" 1 > 2 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 > 1 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 >= 2 ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" 1 >= 1 ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}
	
	@Test
	public void testStringComparison()
	{
		Operand result; 
		result = Expression.evaluate(" '010' < '020' ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" '010' < '010' ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" '010' <= '020' ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" '010' <= '010' ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" '010' > '020' ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" '010' > '010' ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" '010' >= '020' ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" '010' >= '010' ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}
	
	// Missing date comparison where the right hand operand is not a date
	@Test
	public void testDateComparison()
	{
		Operand result; 
		result = Expression.evaluate(" TODAY < TODAY ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" TODAY <= TODAY ", null, null, null);
		Assert.assertTrue((boolean) result.value);
		result = Expression.evaluate(" TODAY > TODAY ", null, null, null);
		Assert.assertFalse((boolean) result.value);
		result = Expression.evaluate(" TODAY >= TODAY ", null, null, null);
		Assert.assertTrue((boolean) result.value);
	}

	// Missing testMoneyComparison,  testPercentageComparison and testClockTimeComparison as I don't know how to express these types without using functions and functions are not available here
}
