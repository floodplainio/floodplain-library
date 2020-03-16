package com.dexels.immutable.impl;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.api.ImmutableTypeParser;
import com.dexels.immutable.factory.ImmutableFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class TestImmutableJSON {
	
	private ImmutableMessageParser parser;
	
	private final static Logger logger = LoggerFactory.getLogger(TestImmutableJSON.class);

	@Before
	public void setup() {
		parser = new JSONImmutableMessageParserImpl();
	}
	@Test
	public void testImmutable() {
		ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", "string").with("testinteger", 3, ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		byte[] bytes = parser.serialize(msg);
		logger.info("TEST: {}", new String(bytes));
	}

	@Test
	public void testDescribe() {
		ImmutableMessage msg = ImmutableFactory.empty().with("teststring", "bla", "string").with("testinteger", 3,ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		String description = parser.describe(msg);
		logger.info("DESCRIPTION: {}",description);
	}

	@Test
	public void testAddSubMessage() {
		ImmutableMessage empty = ImmutableFactory.empty();
		ImmutableMessage created = empty.with("Aap/Noot", 3, ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		Optional<ImmutableMessage> sub = created.subMessage("Aap");
		Assert.assertTrue(sub.isPresent());
		Assert.assertEquals(3,sub.get().value("Noot").get());
	}
	@Test
	public void testGetSubValue() {
		ImmutableMessage empty = ImmutableFactory.empty();
		ImmutableMessage created = empty.with("Aap/Noot", 3, ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		Assert.assertEquals(3,created.value("Aap/Noot").get());
	}
	
	@Test
	public void testSubMessageUsingWith() {
		ImmutableMessage created = ImmutableFactory.empty().with("Aap", 3, ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		ImmutableMessage someOther = ImmutableFactory.empty().with("Noot", 4, ImmutableTypeParser.typeName(ImmutableMessage.ValueType.INTEGER));
		ImmutableMessage combined = created.with("submessage", someOther, ImmutableTypeParser.typeName(ValueType.IMMUTABLE));
		Assert.assertEquals(4,combined.value("submessage/Noot").get());
	}
	

	
	
	@Test
	public void testNdJSON() throws IOException {
		ImmutableMessage m = ImmutableFactory.empty().with("somenumber", 3, "integer");
		logger.info("{}",ImmutableFactory.ndJson(m));

	}
}
