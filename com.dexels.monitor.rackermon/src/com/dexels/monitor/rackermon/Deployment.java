package com.dexels.monitor.rackermon;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "dexels.monitor.deployment", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE, service = {
        Deployment.class })
public class Deployment {

    private final static Logger logger = LoggerFactory.getLogger(Deployment.class);

    private Map<String, Object> deploymentSettings;

    private String name;
    private String clusterName;
    private String type;
    private String group;
    private int availableReplicas;
    private int collisionCount;
    private int readyReplicas;
    private int replicas;
    private int unavailableReplicas;
    private int updatedReplicas;
    private String restartInProgress;

    @Activate
    public void activate(Map<String, Object> settings) {
        try {
            deploymentSettings = settings;

            this.name = (String) settings.get("name");
            this.setType((String) settings.get("type"));
            this.group = (String) settings.get("group");
            this.clusterName = (String) settings.get("cluster");

            setMyReplicaInfo(settings);
            this.setRestartInProgress(settings);

        } catch (Throwable e) {
            logger.error("Error: ", e);
        }

    }

    private void setMyReplicaInfo(Map<String, Object> settings) {
        for (Entry<String, Object> entry : settings.entrySet()) {
            switch (entry.getKey()) {
            case "availablereplicas":
                setAvailableReplicas(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            case "collisioncount":
                setCollisionCount(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            case "readyreplicas":
                setReadyReplicas(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            case "replicas":
                setReplicas(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            case "unavailablereplicas":
                setUnavailableReplicas(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            case "updatedreplicas":
                setUpdatedReplicas(!"null".equals(entry.getValue()) ? Integer.parseInt((String) entry.getValue()) : -1);
                break;
            default:
                break;
            }
        }
    }

    public String getName() {
        return name;
    }

    public String getGroupName() {
        if (group == null) {
            logger.info("I don't have a group? Where do I belong?? {}", name);
            return "unknown";
        }
        return group;
    }

    public int getAvailableReplicas() {
        return availableReplicas;
    }

    public void setAvailableReplicas(int availableReplicas) {
        this.availableReplicas = availableReplicas;
    }

    public int getCollisionCount() {
        return collisionCount;
    }

    public void setCollisionCount(int collisionCount) {
        this.collisionCount = collisionCount;
    }

    public int getReadyReplicas() {
        return readyReplicas;
    }

    public void setReadyReplicas(int readyReplicas) {
        this.readyReplicas = readyReplicas;
    }

    public int getReplicas() {
        return replicas;
    }

    public void setReplicas(int replicas) {
        this.replicas = replicas;
    }

    public int getUnavailableReplicas() {
        return unavailableReplicas;
    }

    public void setUnavailableReplicas(int unavailableReplicas) {
        this.unavailableReplicas = unavailableReplicas;
    }

    public int getUpdatedReplicas() {
        return updatedReplicas;
    }

    public void setUpdatedReplicas(int updatedReplicas) {
        this.updatedReplicas = updatedReplicas;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRestartInProgress() {
        return this.restartInProgress;
    }

    public void setRestartInProgress(String restartInProgress) {
        this.restartInProgress = restartInProgress;
    }

    public void setRestartInProgress(Map<String, Object> settings) {
        int currentEvn = settings.entrySet().stream().filter(entry -> entry.getKey().contains("event-")).collect(Collectors.toList())
                .size();
        // A normal restart is in progress if all the events are of type Normal, the
        // reason is ScalingReplicaSet and the events that occured are 2*times replica
        // (Scaling up and down)x(replicas)
        int nonExp = settings.entrySet().stream().filter(entry -> entry.getKey().contains("event-"))
                .filter(entry -> entry.getKey().contains("source") && "deployment-controller".equals((String) entry.getValue()))
                .filter(entry -> (entry.getKey().contains("type") && !"Normal".equals((String) entry.getValue()))
                        || (entry.getKey().contains("reason") && !"ScalingReplicaSet".equals((String) entry.getValue()))
                        || (entry.getKey().contains("namespace") && !this.getGroupName().equals((String) entry.getValue())))
            .collect(Collectors.toList()).size();
        
        if (currentEvn != 0 && nonExp > 0) {
            logger.warn("Something weird is happening with Deployment " + this.getName());
            this.restartInProgress = "unknown";
            return;
        } 

        if (this.replicas != this.readyReplicas) {
            this.restartInProgress = "true";
        } else {
            this.restartInProgress = "false";
        }
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

}
