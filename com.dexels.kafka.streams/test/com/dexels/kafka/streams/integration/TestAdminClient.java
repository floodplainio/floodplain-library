package com.dexels.kafka.streams.integration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestAdminClient {

	private Map<String,Object> config;
	@Before
	public void setUp() {
		config = new HashMap<>();
		config.put("bootstrap.servers", "10.0.0.7:9092");
	}

	@Test @Ignore
	public void test() throws InterruptedException, ExecutionException {
		try(AdminClient ac = AdminClient.create(config)) {
			CreateTopicsResult cr = ac.createTopics(Arrays.asList("TEST_AAP").stream().map(name->new NewTopic(name, 2, (short)2)).collect(Collectors.toList()));
			cr.all();
			Set<String> topics = ac.listTopics().names().get();
			System.err.println("Topics: "+topics);
		}
	}

}
