package com.dexels.kafka.streams.testdata;

import com.dexels.kafka.streams.api.CoreOperators;
import org.junit.Assert;
import org.junit.Test;


public class TestUngroupKey {

	@Test
	public void test() {
		String key = "aap|noot";
		final String ungrouped = CoreOperators.ungroupKey(key);
		Assert.assertEquals("noot", ungrouped);
		System.err.println(ungrouped);
	}

	@Test
	public void testTriple() {
		String key = "aap|noot|mies";
		final String ungrouped = CoreOperators.ungroupKey(key);
		Assert.assertEquals("mies", ungrouped);
		System.err.println(ungrouped);
	}

}
