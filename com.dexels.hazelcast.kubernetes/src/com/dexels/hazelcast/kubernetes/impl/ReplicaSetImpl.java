package com.dexels.hazelcast.kubernetes.impl;

import java.util.Map;
import java.util.Optional;

import com.dexels.hazelcast.kubernetes.ReplicaSet;

import io.kubernetes.client.models.V1ReplicaSet;

public class ReplicaSetImpl implements ReplicaSet {
	private final String name;
	private final Map<String,String> labels;
	private final Optional<String> parent;

	public ReplicaSetImpl(V1ReplicaSet impl) {
		this.name = impl.getMetadata().getName();
		this.parent = impl.getMetadata().getOwnerReferences().stream().findFirst().map(e->e.getName());
		this.labels = impl.getMetadata().getLabels();
	}
	public ReplicaSetImpl(String name, Optional<String> parent, Map<String,String> labels) {
		this.name = name;
		this.labels = labels;
		this.parent = parent;
	}
	@Override
	public String name() {
		return name;
	}

	@Override
	public Map<String, String> labels() {
		return labels;
	}

	@Override
	public Optional<String> parent() {
		return parent;
	}

}
