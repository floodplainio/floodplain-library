package com.dexels.hazelcast.kubernetes.impl;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.dexels.hazelcast.kubernetes.ClusterMember;

import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Pod;

public class ClusterMemberImpl implements ClusterMember {

	private final String name;
	private final Map<String,String> labels;
	private final List<Integer> ports;
	private final Optional<String> podIp;
	private final String namespace;
	private final String statusPhase;
	private final Optional<String> statusMessage;

	private final Optional<String> statefulSetParent;
	private final Optional<String> replicaSetParent;
	
	public static enum ParentType {
			REPLICASET,STATEFULSET,NONE
	}
	public ClusterMemberImpl(V1Pod base) {
		this.name = base.getMetadata().getName();
		this.labels = base.getMetadata().getLabels();
		this.podIp = Optional.ofNullable(base.getStatus().getPodIP());
		this.statusPhase = base.getStatus().getPhase();
		this.statusMessage =  Optional.ofNullable(base.getStatus().getMessage());
		this.statefulSetParent = base.getMetadata()
			.getOwnerReferences()
			.stream()
			.findFirst()
			.filter(owner->owner.getKind().equals("StatefulSet"))
			.map(owner->owner.getName());
		this.replicaSetParent = base.getMetadata()
				.getOwnerReferences()
				.stream()
				.findFirst()
				.filter(owner->owner.getKind().equals("ReplicaSet"))
				.map(owner->owner.getName());

		this.ports = base.getSpec()
				
				.getContainers()
				.stream()
				.map(V1Container::getPorts)
				.filter(Objects::nonNull)
				.flatMap(e->e.stream())
				.map(e->e.getContainerPort())
				.collect(Collectors.toList());
		this.namespace = base.getMetadata().getNamespace();
	}
	@Override
	public String name() {
		return name;
	}

	@Override
	public String statusPhase() {
		return statusPhase;
	}
	
	@Override
	public Optional<String> statusMessage() {
		return statusMessage;
	}

	@Override
	public Map<String, String> labels() {
		return labels;
	}

	@Override
	public List<Integer> ports() {
		return ports;
	}

	@Override
	public Optional<String> ip() {
		return podIp;
	}
	
	@Override
	public String namespace() {
		return this.namespace;
	}
	
	@Override
	public String toString() {
		return namespace()+" -> "+ "name: "+name+" labels: "+labels+" podIp: "+podIp+" ports: "+ports;
	}

	@Override
	public Optional<String> parent() {
		if(statefulSetParent.isPresent()) {
			return statefulSetParent;
		}
		return replicaSetParent;
	}

	public ParentType parentType() {
		if(statefulSetParent.isPresent()) {
			return ParentType.STATEFULSET;
		}
		if(replicaSetParent.isPresent()) {
			return ParentType.REPLICASET;
		}
		return ParentType.NONE;
	}
}
