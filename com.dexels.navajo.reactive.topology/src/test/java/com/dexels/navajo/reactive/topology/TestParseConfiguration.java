package com.dexels.navajo.reactive.topology;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.StreamTopologyException;
import com.dexels.kafka.streams.api.TopologyContext;


public class TestParseConfiguration {
	
	@Test
	public void testParse() throws IOException {
		try(InputStream r = getClass().getClassLoader().getResourceAsStream("resources.xml")) {
			StreamConfiguration sc = StreamConfiguration.parseConfig("test", r);
			int sinkCount = sc.sinks().size();
			int sourceCount = sc.sources().size();
			System.err.println("Sink: "+sinkCount);
			Assert.assertEquals(3, sinkCount);
			Assert.assertEquals(1, sourceCount);
		}
	}

	@Test
	public void testStart() throws IOException {
		TopologyContext context = new TopologyContext(Optional.of("Generic"), "test", "my_instance", "20200116");

		try(InputStream r = getClass().getClassLoader().getResourceAsStream("resources.xml")) {
			StreamConfiguration sc = StreamConfiguration.parseConfig("test", r);
			sc.startSink(context, sc.source("dvd").orElseThrow(()->new StreamTopologyException("Can't start sink. Unknown sink.")),true);
		}
	}

	
	
}
