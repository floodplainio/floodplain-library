package com.dexels.monitor.rackermon.discovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.monitor.rackermon.Cluster;
import com.dexels.monitor.rackermon.Server;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Component(name = "dexels.monitor.discovery.salt", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class RackermonSaltDiscovery implements Runnable {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final Logger logger = LoggerFactory.getLogger(RackermonSaltDiscovery.class);
    private static final int DEFAULT_RUN_INTERVAL = 30000;
    
    private OkHttpClient client;
    private Set<Cluster> clusters = new HashSet<>();
    private String saltUsername;
    private String saltPassword;
    private String saltServer;
    private String saltPillarTarget;

    private boolean keepRunning = true;
    private int runInterval;
    private String saltToken = "aap";
    private ObjectMapper objectMapper = new ObjectMapper();
    private JsonNode pillar;
    private HazelcastService hazelcastService;
    private ConfigurationAdmin configAdmin;
    
    @Activate
    public void activate(Map<String, Object> setttings) {        
        
        this.saltUsername = (String) setttings.get("saltUsername");
        this.saltPassword = (String) setttings.get("saltPassword");
        this.saltServer = (String) setttings.get("saltServer");
        this.saltPillarTarget = (String) setttings.get("saltPillarTarget");
        
        if (setttings.containsKey("runInterval")) {
            runInterval = new Integer((String)setttings.get("runInterval"));
        } else {
            runInterval = DEFAULT_RUN_INTERVAL;
        }
        
        client = new OkHttpClient();        
        (new Thread(this)).start();
    }

    @Deactivate
    public void deactivate() {
        keepRunning = false;
        clusters.clear();
    }
    
    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "removeCluster", cardinality = ReferenceCardinality.MULTIPLE)
    public void addCluster(Cluster c) {
        clusters.add(c);
    }

    public void removeCluster(Cluster c) {
        clusters.remove(c);
    }
    
    @Reference(name = "HazelcastService", unbind = "clearHazelcastService", policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService service) {
        this.hazelcastService = service;
    }

    public void clearHazelcastService(HazelcastService service) {
        this.hazelcastService = null;
    }
    
    @Reference(unbind = "clearConfigAdmin", policy = ReferencePolicy.DYNAMIC)
    public void setConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = configAdmin;
    }

    public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
        this.configAdmin = null;
    }
    
    private void retrieveServers() {
        try {
            retrievePillar();
            for (Cluster c : clusters) {
                readSaltForClusterMember(c);
            }
        } catch (Throwable t) {
            logger.error("Error running discovery", t);
        }
    }
    
    private void retrievePillar() {
        this.pillar = null; // No more old data
        int response = retrieveSaltPillar();
        if (response == 401) {
            obtainSaltToken();
            response = retrieveSaltPillar();
        }
        // If still not 200, log error
        if (response != 200 || pillar == null) {
            logger.warn("Unable to retrieve pillar - code {}", response);
            throw new IllegalStateException("Unable to retrieve pillar");
        }
    }

    private void readSaltForClusterMember(Cluster c) {
        Iterator<Entry<String, JsonNode>> fields = pillar.get("containers").fields();
        while (fields.hasNext()) {
            Entry<String, JsonNode> entry = fields.next();
            JsonNode container = entry.getValue();
            if (!container.has("env") || !container.get("env").has("CLUSTER") ) {
                continue;
            }
            String containerCluster = container.get("env").get("CLUSTER").asText();
            if (!c.getName().equals(containerCluster)) {
                continue;
            }
            // We found a container belonging to our cluster. Find out where it all lives
            // then compare it to our known locations
            checkContainer(c, entry.getKey());
        }
    }
    
    // Check where containerName all lives on salt, and compare it to Cluster
    private void checkContainer(Cluster c, String containerName) {
        if (c.getServers().isEmpty()) {
            logger.warn("Unable to process checks for empty cluster {}!", c.getName());
            return;
        }
        Set<String> myServers = new HashSet<>();
        Iterator<Entry<String, JsonNode>> allServers = pillar.get("servers").fields();
        List<Server> toRemove = new ArrayList<>();
        List<String> toAdd = new ArrayList<>();
        
        // First we are going to retrieve all servers where this container is supposed to
        // run according to salt
        while (allServers.hasNext()) {
            Entry<String, JsonNode> entry = allServers.next();
            for (final JsonNode serverContainer : entry.getValue()) {
                if (serverContainer.asText().equals(containerName)) {
                    myServers.add(entry.getKey());
                }
            }
        }
        
        // Compare the serverlist with our known servers for this container/servername
        // If we miss any, we should add it
        for (String aServer : myServers) {
            boolean add = true;
            for (Server s : c.getServers()) {
                if (s.getContainerName().equals(containerName) && 
                        s.getServerName().equals(aServer)) {
                    add = false;
                    break;
                }
            }
            if (add) {
                toAdd.add(aServer);
            }
        }
        
        // Check if any of the servers we have, does not exist anymore in salt
        // If this is the case, we should remove it from our config
        for (Server s: c.getServers()) {
            if (!s.getContainerName().equals(containerName) ) {
                continue;
            }
            boolean keep = false;
            for (String serverName : myServers) {
                if (s.getContainerName().equals(containerName) && 
                        s.getServerName().equals(serverName)) {
                    keep = true;
                    break;
                }
            }
            if (!keep) {
                toRemove.add(s);
            }
        }
        Server blueprint = c.getServers().iterator().next();
        for (Server s : toRemove) {
            try {
                logger.info("Removing {} for server {}", s.getContainerName(), s.getServerName());
                Configuration configuration = configAdmin.getConfiguration(s.getServicePid());
                configuration.delete();
            } catch (IOException e) {
                logger.error("Error removing server {}", s.getName(), e);
            }
        }
        for (String newServer : toAdd) {
            try {
                logger.info("Adding {} for server {}", containerName, newServer);

                Configuration newConfig  = configAdmin.createFactoryConfiguration("dexels.monitor.server", null);
                Dictionary<String, Object> properties = new Hashtable<String, Object>();
                properties.put("containerName", containerName);
                properties.put("serverName", newServer);
                properties.put("cluster", c.getName());
                properties.put("maintenance", "true");
                
                String hostname = blueprint.getMyHostname();
                if (hostname != null) {
                    hostname = hostname.replace(blueprint.getContainerName(), containerName);
                    hostname = hostname.replace(blueprint.getServerName(), newServer);
                    properties.put("hostname", hostname);
                }
                newConfig.update(properties);
            } catch (IOException e) {
                logger.error("Error adding server {}", newServer, e);
            }
        }
        
    }

    private int retrieveSaltPillar() {        
        RequestBody formBody = new FormBody.Builder()
                .add("client", "local")
                .add("tgt", saltPillarTarget)
                .add("fun", "pillar.items")
                .build();
        Request request = new Request.Builder()
                .url(saltServer)
                .addHeader("Accept", JSON.toString())
                .addHeader("X-Auth-Token", saltToken)
                .post(formBody)
                .build();

        try {
            Response response = client.newCall(request).execute();
            if (response.code() == 401) {
                return 401;
            }
            
            JsonNode jsonResponse = objectMapper.readTree(response.body().string());
            this.pillar = jsonResponse.get("return").get(0).get(saltPillarTarget);
        } catch (IOException e) {
            logger.error("Error obtaining salt pillar!", e);
            return 500;
        }
        return 200;
        
    }

    private void obtainSaltToken() {        
        String fullUrl = saltServer + "/login";
        
        RequestBody formBody = new FormBody.Builder()
                .add("username", saltUsername)
                .add("password", saltPassword)
                .add("eauth", "pam")
                .build();
        Request request = new Request.Builder()
                .url(fullUrl)
                .addHeader("Accept", JSON.toString())
                .post(formBody)
                .build();

        try {
            Response response = client.newCall(request).execute();
            JsonNode jsonResponse = objectMapper.readTree(response.body().string());
            saltToken = jsonResponse.get("return").get(0).get("token").asText();
        } catch (IOException e) {
            logger.error("Error obtaining token: {}", e);
            return;
        }
        
    }

    @Override
    public void run() {
        obtainSaltToken();

        while (keepRunning) {
            try {
                Thread.sleep(runInterval);
            } catch (InterruptedException e) {
                logger.error("Interrupted thread!");
                keepRunning = false;
                return;
            }
            
            if (isOldestMember()) {
                retrieveServers();
            } else {
                logger.debug("Not oldest member so not discovering anything!");
            }
            
        }
        logger.warn("Stopping discovery Thread! No more discovery");
    }
    
    private boolean isOldestMember() {
        return hazelcastService.oldestMember().equals(hazelcastService.getHazelcastInstance().getCluster().getLocalMember());
    }
}
