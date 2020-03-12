package com.dexels.kafka.webapi;

import static org.junit.Assert.*;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;


public class TestResolveGeneration {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		String s = "@replication";
		 String resolvedDatabaseName = CoreOperators.topicName(s, new TopologyContext(Optional.of("TENANT"), "deployment","total", "generation-12345"));
		 System.err.println("> "+resolvedDatabaseName);
		 
		 assertEquals("TENANT-deployment-generation-12345-total-replication", resolvedDatabaseName);
		
	}

}
