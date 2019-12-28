package com.dexels.kafka.streams.testdata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dexels.kafka.streams.base.Filters;
import com.dexels.kafka.streams.base.StreamConfiguration;
import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

public class TestFilters {

	private ReplicationMessage addressMessage;
	private ReplicationMessage test;
	private ReplicationMessage test_large_number;
	private ReplicationMessage test_small_number;
	private ReplicationMessage testsubfacility;


	@Before
	public void setup() throws IOException {
		StreamConfiguration config = new StreamConfiguration("kafka:9092", Collections.emptyMap(), "", 100, 100, 1);
		new StreamInstance("test", config, Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap());
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("address1.json")) {

			addressMessage = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		}

		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("test.json")) {
			test = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("test_large_number.json")) {
			test_large_number = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("test_small_number.json")) {
			test_small_number = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
		try(InputStream resourceAsStream = TestTransformations.class.getResourceAsStream("testsubfacility.json")) {
			testsubfacility = ReplicationFactory.getInstance().parseStream(resourceAsStream);
		} 
	}
	@Test 
	public void test() {
		Assert.assertTrue(testFilter("greaterthan:somenumber:10","somekey", test));
		Assert.assertTrue(testFilter("lessthan:somenumber:60","somekey", test));
		Assert.assertTrue(testFilter("lessthan:somenumber:60,greaterthan:somenumber:10","somekey", test));

		Assert.assertTrue(testFilter("equalToString:sometext:monkey","somekey", test));
		Assert.assertTrue(testFilter("notEqualToString:sometext:notmonkey","somekey", test));
		Assert.assertFalse(testFilter("equalToString:sometext:alsonotmonkey","somekey", test));
		Assert.assertFalse(testFilter("notEqualToString:sometext:monkey","somekey", test));
		Assert.assertFalse(testFilter("subfacility_facility","somekey", testsubfacility));
	}
	
	@Test
	public void testEqualsAny() {
		Assert.assertTrue(testFilter("equalToAnyIn:zipcode:4565AB:4565AC:4565AD", "", addressMessage));
		Assert.assertTrue(testFilter("equalToAnyIn:zipcode:4565AD:4565AC:4565AB", "", addressMessage));
		Assert.assertFalse(testFilter("equalToAnyIn:zipcode:4565AD:4565AC:4565AA", "", addressMessage));
	}
	
	@Test 
	public void test_multi() {
		Assert.assertTrue(testFilter("lessthan:somenumber:60|greaterthan:somenumber:10","somekey", test_small_number));
		Assert.assertTrue(testFilter("greaterthan:somenumber:60|lessthan:somenumber:10","somekey", test_large_number));
		Assert.assertTrue(testFilter("greaterthan:somenumber:60|lessthan:somenumber:10","somekey", test_small_number));		
		Assert.assertFalse(testFilter("greaterthan:somenumber:60|lessthan:somenumber:10","somekey", test));		
	}

	@Test
	public void testValidZip() {
		Assert.assertTrue(testFilter("validZip:zipcode","somekey", addressMessage));
		Assert.assertTrue(testFilter("validZip:alsozipcode","somekey", addressMessage));
		Assert.assertFalse(testFilter("validZip:notzipcode","somekey", addressMessage));
	}
		
	private boolean testFilter(String definition, String key, ReplicationMessage msg) {
		System.err.println(">>>>> "+msg+" def: "+definition);
		return Filters.getFilter(Optional.of(definition)).get().test(key, msg);
	}
}
