package com.dexels.sharedconfigstore.consul;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.sharedconfigstore.api.SharedConfigurationUpdateListener;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("restriction")
@Component(name = "dexels.sharedconfigstore.consul.configurationwatcher", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ConsulSharedConfigurationWatcher implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ConsulSharedConfigurationWatcher.class);

    private String consulServer;
    private String kvPrefix;
    private String blockIntervalInSeconds;
    private String sourceInstance;
    //private String hostip; 
    
    private ObjectMapper mapper;
    private Integer lastIndex;
    private Map<String, Integer> subIndexes = new HashMap<>();
    private List<SharedConfigurationUpdateListener> listeners = new ArrayList<>();
    private boolean keepRunning;
    private Thread checkThread;

    
    @Activate
    public void activate(Map<String, Object> settings) {
        keepRunning = true;
        mapper = new ObjectMapper();
        Map<String,String> env = System.getenv();
        sourceInstance = getenv(env, "CONTAINERNAME");
        //hostip = getenv(env, "HOSTIP");
        
        consulServer = (String) settings.get("server");
        kvPrefix = (String) settings.get("prefix");
        blockIntervalInSeconds = (String) settings.get("blockIntervalInSeconds");
        checkThread = new Thread(this);
        checkThread.start();
    }

    @Deactivate
    public void deactive() {
        keepRunning = false;

        // Interrupt should stop the current long-polling request
        checkThread.interrupt();
    }

    @Reference(policy = ReferencePolicy.DYNAMIC, name = "ConfigurationListeners", unbind = "removeConfigurationUpdateListener", cardinality = ReferenceCardinality.MULTIPLE)
    public void addConfigurationUpdateListener(SharedConfigurationUpdateListener listener) {
        listeners.add(listener);
    }

    public void removeConfigurationUpdateListener(SharedConfigurationUpdateListener listener) {
        listeners.remove(listener);
    }
    
    

    @Override
    public void run() {

        while (keepRunning) {
            try {
                checkMyConfig();
            } catch (Exception e) {
                logger.error("Exception!!: ", e);
            }
        }
        logger.warn("Stopping ConfigurationWatcher Thread! No more updates");
    }

    public void checkMyConfig() {
        int timeout = Integer.parseInt(blockIntervalInSeconds) + 10;;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpGet get = new HttpGet(getQueryURL());

        try {
            HttpResponse response = client.execute(get);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                // No data yet or something is broken. Lets sleep for a little while to prevent flooding the server
                logger.warn("Got responsecode {}: {}", responseCode, response.getStatusLine().getReasonPhrase());
                Thread.sleep(20000);
                return;
            }

            Integer newLastIndex = Integer.valueOf(response.getHeaders("X-Consul-Index")[0].getValue());
            
            if (lastIndex != null && lastIndex.equals(newLastIndex)) {
                // Nothing new to report
                logger.debug("nothing new to report");
                return;
            }
            lastIndex = newLastIndex;

            String result = rd.readLine();

            JsonNode reply = mapper.readTree(result);
            Iterator<JsonNode> it = reply.elements();
            while (it.hasNext()) {

                JsonNode element = it.next();
                Integer index = element.get("ModifyIndex").asInt();
                String rawKey = element.get("Key").asText();
                
                // Going to strip prefix from key
                String key = rawKey.split(getSearchPrefix() +"/")[1];
                String rawContent = element.get("Value").asText();
                String content = new String(Base64.decodeBase64(rawContent), StandardCharsets.UTF_8);
                notifyListeners(key, index, content);
                
            }
            

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }

    }
    
    
    private void notifyListeners(String key, Integer index, String jsonString) {

        if (subIndexes.get(key) == null || !subIndexes.get(key).equals(index)) {
            subIndexes.put(key, index);
            try {
                Map<String, String> configMap = mapper.readValue(jsonString, new TypeReference<HashMap<String, String>>() {});

                for (SharedConfigurationUpdateListener l : listeners) {
                    try {
                        l.handleConfigurationUpdate(key, configMap);
                    } catch (Exception e) {
                        // Not really my problem...
                        logger.info("Exception on notifying listener: ", e);
                    }

                }
            } catch (IOException e1) {
               logger.error("Exception on parsing JSON value to Map: {} json: {}", e1, jsonString);
            }
        }

    }
    
   
    
    private String getSearchPrefix() {
        return kvPrefix + "/" + sourceInstance;
    }

    private String getQueryURL() {
        if (lastIndex == null) {
            return consulServer + "/v1/kv/" + getSearchPrefix()  + "?recurse";
        }
        return consulServer + "/v1/kv/" + getSearchPrefix()  + "?recurse&index=" + lastIndex + "&wait=" + blockIntervalInSeconds + "s";
    }
    
   

    private final String getenv(Map<String, String> env, String key) {
        String result = env.get(key);
        if(result!=null) {
            return result;
        }
        return System.getProperty(key);
    }
}
