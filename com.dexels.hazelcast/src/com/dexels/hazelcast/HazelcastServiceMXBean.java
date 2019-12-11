package com.dexels.hazelcast;


public interface HazelcastServiceMXBean {

	public String getName();
	public int getClusterSize();
	public String getClusterOverview();
	public String getRegisteredCollections();
	
}
