package com.dexels.hazelcast.kubernetes.impl;

import java.util.Map;

import com.dexels.hazelcast.kubernetes.Cluster;

public class ClusterImpl implements Cluster {
	
	private final String name; 
	private final Map<String,String> labels; 

	public ClusterImpl(String name, Map<String, String> labels) {
		this.name = name;
		this.labels = labels;
	}
	
	@Override
	public String name() {
		return name;
	}
	
	@Override
	public Map<String,String> labels() {
		return labels;
	}

}
