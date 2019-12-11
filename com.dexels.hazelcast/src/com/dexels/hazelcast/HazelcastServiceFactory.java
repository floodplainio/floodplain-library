package com.dexels.hazelcast;

import java.util.Map;

import com.dexels.hazelcast.impl.HazelcastOSGiConfig;
import com.dexels.hazelcast.impl.OSGiHazelcastServiceImpl;


public class HazelcastServiceFactory {

	private HazelcastServiceFactory() {
		// no instances
	}
	// For non-OSGi use:
	private static HazelcastService instance = null;
	
	// For non-OSGi use:
	public static HazelcastService getInstance() {
		return instance;
	}
	
	public static void setInstance(HazelcastService hcs) {
		instance = hcs;
	}

	public static HazelcastService instantiate(Map<String,Object> settings) {
		OSGiHazelcastServiceImpl service = new OSGiHazelcastServiceImpl();
		HazelcastOSGiConfig config = new HazelcastOSGiConfig();
		config.activate(settings);
		service.setHazelcastConfig(config);
		service.manualActivate(settings);
		return service;
	}
}
