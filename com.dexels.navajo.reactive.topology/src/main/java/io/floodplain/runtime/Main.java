package io.floodplain.runtime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.navajo.parser.compiled.ParseException;
import com.dexels.navajo.reactive.CoreReactiveFinder;
import com.dexels.navajo.reactive.api.Reactive;
import com.dexels.navajo.reactive.source.topology.TopologyReactiveFinder;
import com.dexels.navajo.reactive.source.topology.TopologyRunner;

public class Main {

	
	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		String applicationId = "singleRuntime"; // UUID.randomUUID().toString();
		String deployment = "test";
		String storagePath = "storage";
		String generation = "20200219";
		// unsure if I want to keep them around, hard coding for now
		String tenant = "Generic";
		String instance = "myinstance";
		File storage = new File(storagePath);
		storage.mkdirs();
		File currentFolder = new File(System.getProperty("user.dir"));
		File config = new File(currentFolder,"resources.xml");
		ImmutableFactory.setInstance(ImmutableFactory.createParser());
		TopologyContext topologyContext = new TopologyContext(Optional.of(tenant), deployment, instance,generation);
		CoreReactiveFinder finder = new TopologyReactiveFinder();
		Reactive.setFinderInstance(finder);
		StreamConfiguration sc;
		try(InputStream configStream = new FileInputStream(config)) {
			sc = StreamConfiguration.parseConfig("test", configStream);
		}

		TopologyRunner runner = new TopologyRunner(topologyContext,storagePath,applicationId,sc,false);
		runner.runPipeFolder(currentFolder);
	}

}
