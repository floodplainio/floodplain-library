package com.dexels.kubernetes.tree;

import java.util.HashMap;
import java.util.Map;

public class DeploymentNode {
	
	private final Map<String,StatefulSetNode> statefulSets = new HashMap<>();
	private final Map<String,ReplicaSetNode> replicaSets = new HashMap<>();
	
	public Map<String,StatefulSetNode> statefulSet() {
		return this.statefulSets;
		
	}
	public Map<String,ReplicaSetNode> replicaSet() {
		return this.replicaSets;
	}

}
