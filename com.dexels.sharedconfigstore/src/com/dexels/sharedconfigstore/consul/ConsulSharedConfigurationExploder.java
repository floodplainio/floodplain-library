package com.dexels.sharedconfigstore.consul;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


@SuppressWarnings("restriction")
@Component(name = "dexels.sharedconfigstore.consul.configurationexploder", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ConsulSharedConfigurationExploder implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ConsulSharedConfigurationExploder.class);

    private String consulServer;
    private String kvPrefix;
    private String blockIntervalInSeconds;

    private ObjectMapper mapper;
    private boolean keepRunning;
    private Thread explodeThread;
    private Integer configLastIndex;
    private Integer servicesLastIndex;
    private Set<String> navajoContainers = new HashSet<>();


    @Activate
    public void activate(Map<String, Object> settings) {
        mapper = new ObjectMapper();
        keepRunning = true;

        consulServer = (String) settings.get("server");
        kvPrefix = (String) settings.get("prefix");
        blockIntervalInSeconds = (String) settings.get("blockIntervalInSeconds");

        explodeThread = new Thread(this);
        explodeThread.start();
    }

    @Deactivate
    public void deactive() {
        keepRunning = false;

        // Interrupt should stop the current long-polling request
        explodeThread.interrupt();
    }

    @Override
    public void run() {

        while (keepRunning) {
            try {
                checkRawConfig();
                checkContainers();
            } catch (Exception e) {
                logger.error("Exception!!: ", e);
            }
        }
        logger.warn("Stopping ConfigurationWatcher Thread! No more updates");
    }

    public void checkRawConfig() {
        int timeout = Integer.parseInt(blockIntervalInSeconds) + 10;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpGet get = new HttpGet(getQueryURL());

        try {
            HttpResponse response = client.execute(get);

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                // No data yet or something is broken. Lets sleep for a little while to prevent flooding the server
                logger.warn("Got responsecode {}: {}", responseCode, response.getStatusLine().getReasonPhrase());
                Thread.sleep(20000);
                return;
            }

            Integer newLastIndex = Integer.valueOf(response.getHeaders("X-Consul-Index")[0].getValue());

            if (configLastIndex != null && configLastIndex.equals(newLastIndex)) {
                // Nothing new to report
                logger.debug("nothing new to report");
                return;
            }
            configLastIndex = newLastIndex;
            explodeConfig();

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }

    }
    
    public void checkContainers() {
        int timeout = Integer.parseInt(blockIntervalInSeconds) + 10;;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpGet get = new HttpGet(getContainersQueryURL());

        try {
            HttpResponse response = client.execute(get);
            

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                // No data yet or something is broken. Lets sleep for a little while to prevent flooding the server
                logger.warn("Got responsecode {}: {}", responseCode, response.getStatusLine().getReasonPhrase());
                Thread.sleep(20000);
                return;
            }

            Integer newLastIndex = Integer.valueOf(response.getHeaders("X-Consul-Index")[0].getValue());
            
            if (servicesLastIndex != null && servicesLastIndex.equals(newLastIndex)) {
                // Nothing new to report
                logger.debug("nothing new to report");
                return;
            }
            // Remove any current data
            navajoContainers.clear(); 
            
            servicesLastIndex = newLastIndex;
            
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String result = rd.readLine();
            JsonNode reply = mapper.readTree(result);
            Iterator<JsonNode> it = reply.elements();
            while (it.hasNext()) {

                JsonNode element = it.next();
                String containername = element.get("ServiceTags").get(0).asText();
                navajoContainers.add(containername);
            }
            
            explodeConfig();

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }

    }

    private synchronized void explodeConfig() {
        

        // First we going to check for container specific configuration
        Map<String, List<String>> overrides = new HashMap<>();
        for (String container : navajoContainers) {
            Map<String, String> navajoConfig = getConfiguration(container);
            for (String key : navajoConfig.keySet()) {
                List<String> pids = overrides.get(container);
                if (pids == null) {
                    pids = new ArrayList<String>();
                }
                pids.add(key);
                overrides.put(container,  pids);
                createOrUpdateContainerConfiguration(container + "/" + key, navajoConfig.get(key)); 
            }
        }
        
        // Next we fill in the remaining config
        for (String container : navajoContainers) {
            Map<String, String> navajoConfig = getConfiguration(null);
            for (String key : navajoConfig.keySet()) {
                // Check for override
                List<String> pids = overrides.get(container);
                if (pids == null || !pids.contains(key)) {
                    createOrUpdateContainerConfiguration(container + "/" + key, navajoConfig.get(key)); 
                }
            }
        }
        
        
    }
    
    public Map<String, String> getConfiguration(String containername) {
        Map<String, String> result = new HashMap<>();
        
        int timeout = Integer.parseInt(blockIntervalInSeconds) + 10;;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpGet get = new HttpGet(getConfigURL(containername));

        try {
            HttpResponse response = client.execute(get);

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                return result;
            }

            
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String jsonReply = rd.readLine();
            JsonNode reply = mapper.readTree(jsonReply);
            Iterator<JsonNode> it = reply.elements();
            while (it.hasNext()) {

                JsonNode element = it.next();
                String rawKey = element.get("Key").asText();
                String key = null;
                
                // Going to strip prefix from key
                if (containername != null) {
                    key = rawKey.split(getSearchPrefix() +"/" + containername +"/" )[1];
                } else {
                    key = rawKey.split(getSearchPrefix() +"/")[1];
                }
                if (key.contains("/")) {
                    // This is a container-specific configuration object - ignore
                    continue;
                }
                
                String rawContent = element.get("Value").asText();
                String content = new String(Base64.decodeBase64(rawContent), StandardCharsets.UTF_8);
                result.put(key, content);
                
            }

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }
        return result;
    }
    
    public void createOrUpdateContainerConfiguration(String key, String value) {
        int timeout = Integer.parseInt(blockIntervalInSeconds) + 10;;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpPut put = new HttpPut(getPutURL(key));

        HttpEntity entity = null;

        try {
            entity = new ByteArrayEntity(value.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e1) {
            logger.warn("UnsupportedEncodingException exception on getting bytes in UTF8!");

        }
        put.setEntity(entity);

        try {

            HttpResponse response = client.execute(put);
            
            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                logger.error("Got responsecode {}: {}", responseCode, response.getStatusLine().getReasonPhrase());
            }
        } catch (Exception e) {
            logger.debug("Got Exception on performing POST to {}: ", e);
        }

    }
    
    private String getPutURL(String key) {
        Long ts = System.currentTimeMillis();
        StringBuilder b = new StringBuilder();
        b.append(consulServer);
        b.append("/v1/kv/");
        b.append(kvPrefix);
        b.append("/");
        b.append(key);
        b.append("?flags=");
        b.append(ts.toString());
        return b.toString();
    }

    private String getContainersQueryURL() {
        if (servicesLastIndex == null) {
            return consulServer + "/v1/catalog/service/navajo" + kvPrefix;
        }
        return  consulServer + "/v1/catalog/service/navajo" + kvPrefix + "?index=" + servicesLastIndex + "&wait=" + blockIntervalInSeconds + "s";
    }

    private String getQueryURL() {
        if (configLastIndex == null) {
            return consulServer + "/v1/kv/" + getSearchPrefix() + "?recurse";
        }
        return consulServer + "/v1/kv/" + getSearchPrefix() + "?recurse&index=" + configLastIndex + "&wait=" + blockIntervalInSeconds
                + "s";
    }
    
    private String getConfigURL(String container) {
        if (container == null) {
            return consulServer + "/v1/kv/" + getSearchPrefix() + "/?recurse";
        } else {
            return consulServer + "/v1/kv/" + getSearchPrefix() + "/" + container  + "?recurse";
        }
            
      
    }

    private String getSearchPrefix() {
        return kvPrefix + "_raw";
    }

}
