package com.dexels.kubernetes.tree;

import java.util.HashMap;
import java.util.Map;

import com.dexels.hazelcast.kubernetes.ClusterMember;

public class ReplicaSetNode {
	
	private final Map<String,ClusterMember> pods = new HashMap<>();

	public Map<String,ClusterMember> pods() {
		return this.pods;
		
	}
}
