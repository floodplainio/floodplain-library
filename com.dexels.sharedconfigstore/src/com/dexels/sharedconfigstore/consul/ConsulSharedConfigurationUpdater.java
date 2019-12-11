package com.dexels.sharedconfigstore.consul;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.sharedconfigstore.api.SharedConfigurationUpdater;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component(name = "dexels.sharedconfigstore.consul.configurationupdater", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ConsulSharedConfigurationUpdater implements SharedConfigurationUpdater {
    private final static Logger logger = LoggerFactory.getLogger(ConsulSharedConfigurationUpdater.class);

    private String consulServer;
    private String kvPrefix;

    private ObjectMapper mapper;

    @Activate
    public void activate(Map<String, Object> settings) {
        mapper = new ObjectMapper();
        
        consulServer = (String) settings.get("server");
        kvPrefix = (String) settings.get("prefix");
        addConfig();
        

    }

    private void addConfig() {
        new Thread() {

            @Override
            public void run() {
                Map<String, String> config1 =  new HashMap<String, String>();
                Map<String, String> config2 =  new HashMap<String, String>();
                Map<String, String> config3 =  new HashMap<String, String>();
                config1.put("a", "b");
                config1.put("c", "d");
                
                config2.put("a", "b");
                config2.put("c", "d");
                
                config3.put("a", "b");
                config3.put("c", "d");
                try {
                    Thread.sleep(2500);
                    createOrUpdateConfiguration("dexels.comp0", null, config1);
                    Thread.sleep(10000);
                    createOrUpdateConfiguration("dexels.comp1", "test-knvb-1", config2);
                    Thread.sleep(10000);
                    createOrUpdateConfiguration("dexels.comp0", "test-knvb-1", config2);
                    Thread.sleep(10000);
                    createOrUpdateConfiguration("dexels.configstore.testcomponent",null, config3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

    }

    @Override
    public void createOrUpdateConfiguration(String pid, String container, Map<String, String> settings) {
        int timeout = 30;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpPut put = new HttpPut(getPutURL(pid, container));

        HttpEntity entity = null;
        String jsonEncoded = null;;
        try {
            jsonEncoded = mapper.writeValueAsString(settings);
        } catch (IOException e2) {
            e2.printStackTrace();
        }
        try {
            entity = new ByteArrayEntity(jsonEncoded.getBytes("UTF-8"));
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


    private String getPutURL(String pid, String container) {
        Long ts = System.currentTimeMillis();
        StringBuilder b = new StringBuilder();
        b.append(consulServer);
        b.append("/v1/kv/");
        b.append(kvPrefix);
        b.append("_raw");
        b.append("/");
        if (container != null && !container.equals("")) {
            b.append(container);
            b.append("/");
        }
       
        b.append(pid);
        b.append("?flags=");
        b.append(ts.toString());
        return b.toString();
    }

}
