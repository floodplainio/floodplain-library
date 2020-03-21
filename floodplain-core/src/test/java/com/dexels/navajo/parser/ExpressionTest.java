package com.dexels.navajo.parser;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.navajo.document.ExpressionEvaluator;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.parser.compiled.api.CachedExpressionEvaluator;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.factory.ReplicationFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.dexels.immutable.api.ImmutableMessage.ValueType;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class ExpressionTest {

//	private Navajo testDoc;
//	private Message topMessage;
	private ImmutableMessage immutableMessage;
	private ImmutableMessage paramMessage;

	ExpressionEvaluator expressionEvaluator = new CachedExpressionEvaluator();

	@Before
	public void setup() {
//		testDoc = NavajoFactory.getInstance().createNavajo();
//		topMessage = NavajoFactory.getInstance().createMessage(testDoc, "MyTop");
//		testDoc.addMessage(topMessage);
//		Property pt = NavajoFactory.getInstance().createProperty(testDoc,
//				"TopProp", "1", "", Property.DIR_IN);
//		testSelection = NavajoFactory.getInstance().createSelection(testDoc, "option1", "value1", true);
//		pt.addSelection(testSelection);
//		topMessage.addProperty(pt);
//		Message a = NavajoFactory.getInstance().createMessage(testDoc,
//				"MyArrayMessage", "array");
//		topMessage.addMessage(a);
//		for (int i = 0; i < 5; i++) {
//			Message a1 = NavajoFactory.getInstance().createMessage(testDoc,
//					"MyArrayMessage");
//			a.addMessage(a1);
//			Property p = NavajoFactory.getInstance().createProperty(testDoc,
//					"MyProp", "string", "noot" + i, 0, "", "in");
//			a1.addProperty(p);
//			Property p2 = NavajoFactory.getInstance().createProperty(testDoc,
//					"MyProp2", "string", "aap" + i, 0, "", "in");
//			a1.addProperty(p2);
//		}
//
		Map<String,Object> values = new HashMap<>();
		Map<String, ValueType> types = new HashMap<>();
		values.put("SomeString", "Tralala");
		types.put("SomeString", ValueType.STRING);
		values.put("SomeInteger", 3);
		types.put("SomeInteger", ValueType.INTEGER);
		immutableMessage = ReplicationFactory.createReplicationMessage(Optional.empty(),Optional.empty(),Optional.empty(), null, 0, Operation.NONE, Collections.emptyList(), types, values, Collections.emptyMap(), Collections.emptyMap(),Optional.empty(),Optional.empty()).message();
		
		Map<String,Object> valueparams = new HashMap<>();
		Map<String,ValueType> typeparams = new HashMap<>();
		valueparams.put("SomeString", "Tralala2");
		typeparams.put("SomeString", ValueType.STRING);
		valueparams.put("SomeInteger", 4);
		typeparams.put("SomeInteger", ValueType.INTEGER);
		paramMessage = ReplicationFactory.createReplicationMessage(Optional.empty(),Optional.empty(),Optional.empty(),null, 0, Operation.NONE, Collections.emptyList(), typeparams, valueparams, Collections.emptyMap(), Collections.emptyMap(),Optional.empty(),Optional.empty()).message();


	}
	@Test
	public void testExpression() throws Exception {

		Operand o = expressionEvaluator.evaluate("1+1");
		assertEquals(2, o.value);

//		o = expressionEvaluator.evaluate("TODAY + 0#0#2#0#0#0", null, null,null);
//		System.err.println(o.value);



	}

	@Test
	public void testUnicode() throws Exception {
		Operand o = expressionEvaluator.evaluate("'ø'+'æ'");
		assertEquals("øæ", o.value);
	}

	@Test
	public void testExpressionNewlineOutside() throws Exception {
		Operand o = expressionEvaluator.evaluate("1\n+\n1");
		assertEquals(2, o.value);
	}

	@Test
	public void testExpressionNewline() throws Exception {
		Operand o = expressionEvaluator.evaluate("'aap\nnoot'");
		assertEquals("aap\nnoot", o.value);
	}
	
	@Test
	public void testNonAscii() throws Exception {
		Operand o = expressionEvaluator.evaluate("'àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž'+'àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž'", Optional.empty(),Optional.empty());
		assertEquals("àáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżžàáâãäåāăąæßçćĉċčèéêëēĕėęěĝğġģĥħìíîïĩīĭıįĵķĸĺļľŀłñńņňŋòóôöõøōŏőœŕŗřśŝşšţťŧùúûüũůūŭűųŵýÿŷźżž", o.value);
	}
	


	@Test
	public void testExpressionWithImmutableMessage() throws Exception {
		Operand o = Expression.evaluate("[SomeInteger]" ,Optional.of(immutableMessage),Optional.of(paramMessage));
		assertEquals(3, o.value);
	}

	@Test
	public void testExpressionWithImmutableParamMessage() throws Exception {
		Operand o = Expression.evaluate("[@SomeInteger]" ,Optional.of(immutableMessage),Optional.of(paramMessage));
		assertEquals(4, o.value);
	}

	@Test
	public void testListWithMultipleTypes() throws Exception {
		Operand o = Expression.evaluate("{1,'a',2+2}");
		List<Object> res = (List<Object>) o.value;
		assertEquals(3, res.size());
	}

	@Test
	public void testListWithSingleElement() throws Exception {
		Operand o = Expression.evaluate("{'a'}");
		List<Object> res = (List<Object>) o.value;
		assertEquals(1, res.size());
	}

	@Test
	public void testImmutableTMLPath() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, ValueType.INTEGER);
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[sub/innerint]", Optional.of(combined), Optional.empty());
		int s = o.integerValue();
		assertEquals(3, s);
	}

	@Test
	public void testTrailingTMLPath() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, ValueType.INTEGER);
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[sub/]", Optional.of(combined), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(3, s.value("innerint").get());
	}

	@Test
	public void testTrailingTMLPathParam() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		ImmutableMessage inner = ImmutableFactory.empty().with("innerint", 3, ValueType.INTEGER);
		
		ImmutableMessage combined = outer.withSubMessage("sub", inner);
		Operand o = Expression.evaluate("[@sub/]", Optional.empty(), Optional.of(combined));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(3, s.value("innerint").get());
	}

	@Test
	public void testTrailingTMLPathParamList() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		ImmutableMessage inner1 = ImmutableFactory.empty().with("innerint", 1, ValueType.INTEGER);
		ImmutableMessage inner2 = ImmutableFactory.empty().with("innerint", 2, ValueType.INTEGER);
		ImmutableMessage inner3 = ImmutableFactory.empty().with("innerint", 3, ValueType.INTEGER);
		
		List<ImmutableMessage> subList = Arrays.asList(inner1,inner2,inner3);
		ImmutableMessage combined = outer.withSubMessages("sub", subList);
		ImmutableMessage incoming = ImmutableFactory.empty();
		Operand o = Expression.evaluate("[@sub/]", Optional.of(incoming), Optional.of(combined));
		List<ImmutableMessage> s = o.immutableMessageList();
		assertEquals(3, s.size());
	}

	
	@Test
	public void testEmptyTML() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		Operand o = Expression.evaluate("[]", Optional.of(outer), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
	
	@Test
	public void testEmptyTMLJustSlash() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		Operand o = Expression.evaluate("[/]", Optional.of(outer), Optional.empty());
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}

	@Test
	public void testEmptyTMLParam() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		Operand o = Expression.evaluate("[@]", Optional.empty(), Optional.of(outer));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
	
	@Test
	public void testEmptySlashTMLParam() throws Exception {
		ImmutableMessage outer = ImmutableFactory.empty().with("outerint", 1, ValueType.INTEGER);
		Operand o = Expression.evaluate("[/@]", Optional.empty(), Optional.of(outer));
		ImmutableMessage s = o.immutableMessageValue();
		assertEquals(outer, s);
	}
}

