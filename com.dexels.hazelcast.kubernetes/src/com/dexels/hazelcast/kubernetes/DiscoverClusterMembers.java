package com.dexels.hazelcast.kubernetes;

import java.util.List;

public interface DiscoverClusterMembers {
	public List<String> discoverPods(String namespace, String cluster, int containerPort);
	public List<ClusterMember> listPods(String namespace);
	public List<String> listNamespaces();
	public String currentNamespace();
	public List<Cluster> listServices(String namespace);
	public List<Deployment> listDeployments(String namespace);
	public List<StatefulSet> listStatefulSets(String namespace);
	public List<ReplicaSet> listReplicaSets(String namespace);
}
