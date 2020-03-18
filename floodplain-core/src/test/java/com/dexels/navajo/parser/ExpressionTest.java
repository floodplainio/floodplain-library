package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.*;
import com.dexels.navajo.parser.compiled.api.CachedExpressionEvaluator;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class ExpressionTest {

	private Navajo testDoc;
	private Message topMessage;
	private Selection testSelection;
	private ImmutableMessage immutableMessage;
	private ImmutableMessage paramMessage;


	@Before
	public void setup() {
		NavajoFactory.getInstance().setExpressionEvaluator(
				new CachedExpressionEvaluator());
		
		testDoc = NavajoFactory.getInstance().createNavajo();
		topMessage = NavajoFactory.getInstance().createMessage(testDoc, "MyTop");
		testDoc.addMessage(topMessage);
		Property pt = NavajoFactory.getInstance().createProperty(testDoc,
				"TopProp", "1", "", Property.DIR_IN);
		testSelection = NavajoFactory.getInstance().createSelection(testDoc, "option1", "value1", true);
		pt.addSelection(testSelection);
		topMessage.addProperty(pt);
		Message a = NavajoFactory.getInstance().createMessage(testDoc,
				"MyArrayMessage", "array");
		topMessage.addMessage(a);
		for (int i = 0; i < 5; i++) {
			Message a1 = NavajoFactory.getInstance().createMessage(testDoc,
					"MyArrayMessage");
			a.addMessage(a1);
			Property p = NavajoFactory.getInstance().createProperty(testDoc,
					"MyProp", "string", "noot" + i, 0, "", "in");
			a1.addProperty(p);
			Property p2 = NavajoFactory.getInstance().createProperty(testDoc,
					"MyProp2", "string", "aap" + i, 0, "", "in");
			a1.addProperty(p2);
		}
		
		Map<String,Object> values = new HashMap<>();
		Map<String,String> types = new HashMap<>();
		values.put("SomeString", "Tralala");
		types.put("SomeString", "string");
		values.put("SomeInteger", 3);
		types.put("SomeInteger", "integer");
		immutableMessage = ReplicationFactory.createReplicationMessage(Optional.empty(),Optional.empty(),Optional.empty(), null, 0, Operation.NONE, Collections.emptyList(), types, values, Collections.emptyMap(), Collections.emptyMap(),Optional.empty(),Optional.empty()).message();
		
		Map<String,Object> valueparams = new HashMap<>();
		Map<String,String> typeparams = new HashMap<>();
		valueparams.put("SomeString", "Tralala2");
		typeparams.put("SomeString", "string");
		valueparams.put("SomeInteger", 4);
		typeparams.put("SomeInteger", "integer");
		paramMessage = ReplicationFactory.createReplicationMessage(Optional.empty(),Optional.empty(),Optional.empty(),null, 0, Operation.NONE, Collections.emptyList(), typeparams, valueparams, Collections.emptyMap(), Collections.emptyMap(),Optional.empty(),Optional.empty()).message();


	}
	@Test
	public void testExpression() throws Exception {
		ExpressionEvaluator ee = NavajoFactory.getInstance()
				.getExpressionEvaluator();

		Operand o = ee.evaluate("1+1", null, null,null);
		assertEquals(2, o.value);

//		o = ee.evaluate("TODAY + 0#0#2#0#0#0", null, null,null);
//		System.err.println(o.value);



	}

	@Test
	public void testUnicode() throws Exception {
		ExpressionEvaluator ee = NavajoFactory.getInstance().getExpressionEvaluator();
		Operand o = ee.evaluate("'ø'+'æ'", null, null,null);
		assertEquals("øæ", o.value);
	}

	@Test
	public void testExpressionNewlineOutside() throws Exception {
		ExpressionEvaluator ee = NavajoFactory.getInstance()
				.getExpressionEvaluator();

		Operand o = ee.evaluate("1\n+\n1", null, null,null);
		assertEquals(2, o.value);
	}

	@Test
	public void testExpressionNewline() throws Exception {
		ExpressionEvaluator ee = NavajoFactory.getInstance()
				.getExpressionEvaluator();

		Operand o = ee.evaluate("'aap\nnoot'", null, null,null);
		assertEquals("aap\nnoot", o.value);
	}
	
	@Test
	public void testNonAscii() throws Exception {
		ExpressionEvaluator ee = NavajoFactory.getInstance()
				.getExpressionEvaluator();

		Operand o = ee.evaluate("'àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž'+'àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž'", null,null,null);
		assertEquals("àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżžàáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž", o.value);
	}
	


	@Test
	public void testExpressionWithImmutableMessage() throws Exception {
		Expression.compileExpressions = true;
		Operand o = Expression.evaluate("[SomeInteger]", null ,Optional.of(immutableMessage),Optional.of(paramMessage));
		assertEquals(3, o.value);
	}

	@Test
	public void testExpressionWithImmutableParamMessage() throws Exception {
		Expression.compileExpressions = true;
		Operand o = Expression.evaluate("[@SomeInteger]", null ,Optional.of(immutableMessage),Optional.of(paramMessage));
		assertEquals(4, o.value);
	}

	@Test
	public void testListWithMultipleTypes() throws Exception {
		Expression.compileExpressions = true;
		Operand o = Expression.evaluate("{1,'a',2+2}");
		List<Object> res = (List<Object>) o.value;
		assertEquals(3, res.size());
	}

	@Test
	public void testListWithSingleElement() throws Exception {
		Expression.compileExpressions = true;
		Operand o = Expression.evaluate("{'a'}");
		List<Object> res = (List<Object>) o.value;
		assertEquals(1, res.size());
	}

	@Test
	public void testImmutableTMLPath() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, "integer");
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[sub/innerint]", null, Optional.of(combined), Optional.empty());
		int s = o.integerValue();
		assertEquals(3, s);
	}

	@Test
	public void testTrailingTMLPath() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, "integer");
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[sub/]", null, Optional.of(combined), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(3, s.value("innerint").get());
	}

	@Test
	public void testTrailingTMLPathParam() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, "integer");
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[@sub/]", null, Optional.empty(), Optional.of(combined));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(3, s.value("innerint").get());
	}

	@Test
	public void testTrailingTMLPathParamList() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		ImmutableMessage inner1 = ImmutableFactory.empty().with("innerint", 1, "integer");
		ImmutableMessage inner2 = ImmutableFactory.empty().with("innerint", 2, "integer");
		ImmutableMessage inner3 = ImmutableFactory.empty().with("innerint", 3, "integer");
		
		List<ImmutableMessage> subList = Arrays.asList(inner1,inner2,inner3);
		ImmutableMessage combined = outer.withSubMessages("sub", subList);
		ImmutableMessage incoming = ImmutableFactory.empty();
		Operand o = Expression.evaluate("[@sub/]", null, Optional.of(incoming), Optional.of(combined));
		List<ImmutableMessage> s = o.immutableMessageList();
		assertEquals(3, s.size());
	}

	
	@Test
	public void testEmptyTML() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		Operand o = Expression.evaluate("[]", null, Optional.of(outer), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
	
	@Test
	public void testEmptyTMLJustSlash() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		Operand o = Expression.evaluate("[/]", null, Optional.of(outer), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}

	@Test
	public void testEmptyTMLParam() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		Operand o = Expression.evaluate("[@]", null, Optional.empty(), Optional.of(outer));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
	
	@Test
	public void testEmptySlashTMLParam() throws Exception {
		Expression.compileExpressions = true;
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, "integer");
		Operand o = Expression.evaluate("[/@]", null, Optional.empty(), Optional.of(outer));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
}

