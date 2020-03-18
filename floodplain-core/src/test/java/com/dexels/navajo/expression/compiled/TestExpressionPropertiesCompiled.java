package com.dexels.navajo.expression.compiled;

import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.parser.compiled.api.CachedExpressionEvaluator;
import org.junit.Before;

public class TestExpressionPropertiesCompiled {

	private Navajo testDoc;
	private Message topMessage;
	private Property one;
	private Property two;
	private Property three;
	private Property five;

	@Before
	public void setup() {
		testDoc = NavajoFactory.getInstance().createNavajo();
		topMessage = NavajoFactory.getInstance().createMessage(testDoc, "MyTop");
		testDoc.addMessage(topMessage);
		one = NavajoFactory.getInstance().createProperty(testDoc,
				"One", Property.INTEGER_PROPERTY, "1", 0, "", Property.DIR_OUT);
		two = NavajoFactory.getInstance().createProperty(testDoc,
				"Two", Property.EXPRESSION_PROPERTY, "[One]+1", 0, "", Property.DIR_OUT);
		three = NavajoFactory.getInstance().createProperty(testDoc,
				"Three", Property.EXPRESSION_PROPERTY, "2+1", 0, "", Property.DIR_OUT);
		five = NavajoFactory.getInstance().createProperty(testDoc,
				"Five", Property.EXPRESSION_PROPERTY, "[Two]+[Three]", 0, "", Property.DIR_OUT);
		topMessage.addProperty(one);
		topMessage.addProperty(two);
		topMessage.addProperty(three);
		topMessage.addProperty(five);
	}

}
