package io.floodplain.kafka.webapi;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

public class GroupStructure {

    public class Tenant {
        @JsonProperty
        private Map<String, Deployment> deployments = new HashMap<>();

        @JsonProperty
        private Map<String, Instance> instances = new HashMap<>();

        public Tenant() {
        }

        public Deployment getDeployment(String name) {
            Deployment d = deployments.get(name);
            if (d == null) {
                final Deployment deployment = new Deployment();
                deployments.put(name, deployment);
                d = deployment;
            }
            return d;
        }

        public Instance getInstance(String name) {
            Instance d = instances.get(name);
            if (d == null) {
                final Instance instance = new Instance();
                instances.put(name, instance);
                d = instance;
            }
            return d;
        }
    }

    public class Instance {
        @JsonProperty
        private final Map<String, Workflow> workflows = new HashMap<>();

        public Workflow getWorkflow(String workflowName) {
            Workflow w = workflows.get(workflowName);
            if (w == null) {
                w = new Workflow();
                workflows.put(workflowName, w);
            }
            return w;
        }

    }

    public class Deployment {
        @JsonProperty
        private final Map<String, Generation> generations = new HashMap<>();

        @JsonProperty
        private final Set<String> nonGenerationalGroups = new HashSet<>();

        public Deployment() {

        }

        public Set<String> getNonGenerationalGroups() {
            return Collections.unmodifiableSet(this.nonGenerationalGroups);
        }

        public void addNonGenerationalGroup(String topic) {
            nonGenerationalGroups.add(topic);
        }

        public Generation getGeneration(String generationName) {
            Generation g = generations.get(generationName);
            if (g == null) {
                g = new Generation();
                generations.put(generationName, g);
            }
            return g;
        }

    }

    public class Generation {
        @JsonProperty
        private final Set<String> groups = new HashSet<>();

        public void addGroup(String group) {
            this.groups.add(group);
        }

        public Set<String> getGroups() {
            return Collections.unmodifiableSet(this.groups);
        }

        @JsonProperty
        public int groupCount() {
            return this.groups.size();
        }
    }

    public class Workflow {
        @JsonProperty
        private final Set<String> ids = new HashSet<>();

        public void addId(String id) {
            ids.add(id);
        }

        public Set<String> getIds() {
            return Collections.unmodifiableSet(ids);
        }
    }

    @JsonProperty
    private Map<String, Tenant> tenants = new HashMap<>();

    @JsonProperty
    private Set<String> otherGroups = new HashSet<>();

    public Collection<Tenant> tenants() {
        return tenants.values();
    }

    public Tenant getTenant(String tenantName) {
        Tenant t = tenants.get(tenantName);
        if (t == null) {
            t = new Tenant();
            tenants.put(tenantName, t);
        }
        return t;
    }

    public GroupStructure consumeGroup(String groupOriginal) {
        String group;
        if (groupOriginal.startsWith("highlevel-")) {
            group = groupOriginal.substring("highlevel-".length(), groupOriginal.length());
        } else if (groupOriginal.startsWith("lowlevel-")) {
            group = groupOriginal.substring("lowlevel-".length(), groupOriginal.length());
        } else {
            group = groupOriginal;
        }
        if (groupOriginal.indexOf("-workflow-") != -1) {
//			return 
            addWorkflowGroup(groupOriginal);
            return this;
        }
        if (groupOriginal.startsWith("SubscribeAdapter")) {
            addOtherGroup(groupOriginal);
            return this;
        }
        String[] parts = group.split("-");
        if (parts.length < 3 || group.startsWith("NAVAJO-")) {
            addOtherGroup(groupOriginal);
        } else {
            Deployment d = getTenant(parts[0]).getDeployment(parts[1]);
            if (parts[0].equals("connect")) {
                if (parts[3].equals("generation")) {
                    d.getGeneration(parts[4]).addGroup(groupOriginal);
                } else {
                    d.addNonGenerationalGroup(groupOriginal);
                }
            } else {
                if (parts[2].equals("generation")) {
                    d.getGeneration(parts[3]).addGroup(groupOriginal);
                } else {
                    d.addNonGenerationalGroup(groupOriginal);
                }
            }
        }
        return this;
    }

    private void addOtherGroup(String groupName) {
        otherGroups.add(groupName);
    }

    private void addWorkflowGroup(String groupName) {
        String[] parts = groupName.split("-workflow-");
        if (parts.length < 2) {
            System.err.println("Error, not enough parts: " + groupName + " ignoring");
            return;
        }
        String instance = parts[0];
        String[] workflowParts = parts[1].split("-");
        String tenant = workflowParts[1];
        String workflow = workflowParts[0];
        String id = workflowParts[2];
        getTenant(tenant).getInstance(instance).getWorkflow(workflow).addId(id);

    }

    public Set<String> tenantNames() {
        return tenants.keySet();
    }


}
