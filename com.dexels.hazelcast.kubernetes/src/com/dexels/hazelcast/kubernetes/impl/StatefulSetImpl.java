package com.dexels.hazelcast.kubernetes.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dexels.hazelcast.kubernetes.Deployment;
import com.dexels.hazelcast.kubernetes.Event;
import com.dexels.hazelcast.kubernetes.StatefulSet;

import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1StatefulSet;

public class StatefulSetImpl implements StatefulSet {
	
	private final String name; 
	private final Map<String,String> labels; 
    private final Map<String, String> status;
    private final Map<String, String> templateLabels;
    private final List<Event> events;

    public StatefulSetImpl(String name, Map<String, String> labels, Map<String, String> templateLabels, V1StatefulSet statefulSet,
            List<Event> events) {
		this.name = name;
		this.labels = labels;
        this.templateLabels = templateLabels;
        this.status = new HashMap<String, String>();
        this.events = events;
        this.status.put("availablereplicas",
                statefulSet.getStatus().getCurrentReplicas() != null ? String.valueOf(statefulSet.getStatus().getCurrentReplicas())
                        : "null");
        this.status.put("collisioncount",
                statefulSet.getStatus().getCollisionCount() != null ? String.valueOf(statefulSet.getStatus().getCollisionCount())
                        : "null");
        this.status.put("readyreplicas",
                statefulSet.getStatus().getReadyReplicas() != null ? String.valueOf(statefulSet.getStatus().getReadyReplicas())
                        : "null");
        this.status.put("replicas",
                statefulSet.getStatus().getReplicas() != null ? String.valueOf(statefulSet.getStatus().getReplicas()) : "null");
//        this.status.put("unavailablereplicas",
//                statefulSet.getStatus().getUnavailableReplicas() != null
//                        ? String.valueOf(statefulSet.getStatus().getUnavailableReplicas())
//                        : "null");
        this.status.put("updatedreplicas",
                statefulSet.getStatus().getUpdatedReplicas() != null ? String.valueOf(statefulSet.getStatus().getUpdatedReplicas())
                        : "null");
        
    }
	
	@Override
	public String name() {
		return name;
	}
	
	@Override
	public Map<String,String> labels() {
		return labels;
	}
	
	@Override
    public Map<String,String> templateLabels() {
        return templateLabels;
    }

    @Override
    public Map<String, String> status() {
        return status;
    }

    @Override
    public List<Event> events() {
        return events;
    }

    @Override
    public Map<String, String> eventsMap() {
        HashMap<String, String> events = new HashMap<String, String>();
        for (Event event : events()) {
            events.put("event-" + event.uid() + "-message", event.message());
            events.put("event-" + event.uid() + "-type", event.type());
            events.put("event-" + event.uid() + "-reason", event.reason());
            events.put("event-" + event.uid() + "-namespace", event.namespace());
            events.put("event-" + event.uid() + "-source", event.source());
        }
        return events;
    }

}
