package com.dexels.kubernetes.tree;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dexels.hazelcast.kubernetes.Deployment;
import com.dexels.hazelcast.kubernetes.ReplicaSet;
import com.dexels.hazelcast.kubernetes.StatefulSet;

public class ContextNode {
	
	public void build(List<Deployment> deployments, List<ReplicaSet> replicaSets, List<StatefulSet> statefulSets) {
		Map<String,Deployment> depMap = deployments.stream().collect(Collectors.toMap(depl->depl.name(), depl->depl));
		Map<String,ReplicaSet> replMap = replicaSets.stream().collect(Collectors.toMap(repl->repl.name(), repl->repl));
		Map<String,StatefulSet> statefulMap = statefulSets.stream().collect(Collectors.toMap(statefulS->statefulS.name(), statefulS->statefulS));

//		statefulMap.entrySet().forEach(e->{
//			e.getValue()deployments;
//		});
	}

}
