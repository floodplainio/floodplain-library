package com.dexels.kafka.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class TopicStructure {
	
	public class Tenant {
		@JsonProperty
		private Map<String,Deployment> deployments = new HashMap<>();
		public Tenant() {
		}
		
		public Deployment getDeployment(String name) {
			Deployment d = deployments.get(name);
			if(d==null) {
				final Deployment deployment = new Deployment();
				deployments.put(name, deployment);
				d = deployment;
			}
			return d;
		}
	}
	
	public class Deployment {
		@JsonProperty
		private final Map<String,Generation> generations = new HashMap<>();

		@JsonProperty
		private final Set<String> nonGenerationalTopics = new HashSet<>();
		
		public Deployment() {
			
		}
		public Set<String> getNonGenerationalTopics() {
			return Collections.unmodifiableSet(this.nonGenerationalTopics);
		}
		
		public void addNonGenerationalTopic(String topic) {
			nonGenerationalTopics.add(topic);
		}
		
		public Generation getGeneration(String generationName) {
			Generation g = generations.get(generationName);
			if(g==null) {
				g = new Generation();
				generations.put(generationName, g);
			}
			return g;
		}
	}
	
	public class Generation {
		@JsonProperty
		private final Set<String> topics = new HashSet<>();
		
		public void addTopic(String topic) {
			this.topics.add(topic);
		}
		
		public Set<String> getTopics() {
			return Collections.unmodifiableSet(this.topics);
		}
	}
	
	@JsonProperty
	private Map<String,Tenant> tenants = new HashMap<>();

	@JsonProperty
	private Set<String> otherTopics = new HashSet<>();
	
	public Collection<Tenant> tenants() {
		return tenants.values();
	}

	public Tenant getTenant(String tenantName) {
		Tenant t = tenants.get(tenantName);
		if(t==null) {
			t = new Tenant();
			tenants.put(tenantName, t);
		}
		return t;
	}

	public TopicStructure consumeTopic(String topicOriginal) {
		String topic;
		if(topicOriginal.startsWith("highlevel-")) {
			topic = topicOriginal.substring("highlevel-".length(), topicOriginal.length());
		} else if (topicOriginal.startsWith("lowlevel-")) {
			topic = topicOriginal.substring("lowlevel-".length(), topicOriginal.length());
		} else {
			topic = topicOriginal;
		}
		String[] parts = topic.split("-");
		if(parts.length<3 || topic.startsWith("NAVAJO-")) {
			otherTopics.add(topicOriginal);
		} else {
			Deployment d = getTenant(parts[0]).getDeployment(parts[1]);
			if(parts[2].equals("generation")) {
				d.getGeneration(parts[3]).addTopic(topicOriginal);
			} else {
				d.addNonGenerationalTopic(topicOriginal);
			}
		}
		return this;
	}

	public Set<String> tenantNames() {
		return tenants.keySet();
	}

	
}
