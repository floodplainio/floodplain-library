package com.dexels.sharedconfigstore.consul;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.sharedconfigstore.api.SharedConfigurationQuerier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("restriction")
@Component(name = "dexels.sharedconfigstore.consul.configurationquerier", immediate = true, configurationPolicy = ConfigurationPolicy.REQUIRE)
public class ConsulSharedConfigurationQuerier implements SharedConfigurationQuerier {
    private final static Logger logger = LoggerFactory.getLogger(ConsulSharedConfigurationQuerier.class);
    private String consulServer;
    private String kvPrefix;
    private ObjectMapper mapper;

    @Activate
    public void activate(Map<String, Object> settings) {
        mapper = new ObjectMapper();

        consulServer = (String) settings.get("server");
        kvPrefix = (String) settings.get("prefix");
        logger.info("activated");
        logger.info(getContainers() + "");
        logger.info("getConfig: " + getConfiguration("dexels.comp0", "test-knvb-1"));
        logger.info("getConfig: " + getConfiguration("dexels.comp0", null));

    }

    @Override
    public Set<String> getContainers() {
        Set<String> navajoContainers = new HashSet<>();

        int timeout = 30;
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
                return navajoContainers;
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String result = rd.readLine();
            JsonNode reply = mapper.readTree(result);
            Iterator<JsonNode> it = reply.elements();
            while (it.hasNext()) {

                JsonNode element = it.next();
                String containername = element.get("ServiceTags").get(0).asText();
                navajoContainers.add(containername);
            }

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }
        return navajoContainers;
    }

    @Override
    public String getConfiguration(String pid, String containername) {
        String result = null;

        int timeout = 30;
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
                .setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

        HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
        HttpGet get = new HttpGet(getConfigURL(pid, containername));

        try {
            HttpResponse response = client.execute(get);

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode >= 300) {
                return result;
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            String jsonReply = rd.readLine();
            JsonNode reply = mapper.readTree(jsonReply);
            JsonNode element = reply.elements().next();

            String rawContent = element.get("Value").asText();
            String content = new String(Base64.decodeBase64(rawContent), StandardCharsets.UTF_8);
            result = content;

        } catch (Exception e) {
            logger.error("Got Exception on performing GET: ", e);
        }
        return result;
    }

    private String getContainersQueryURL() {
        return consulServer + "/v1/catalog/service/navajo" + kvPrefix;

    }

    private String getConfigURL(String pid, String container) {
        if (container == null) {
            return consulServer + "/v1/kv/" + getSearchPrefix() + "/" + pid;
        } else {
            return consulServer + "/v1/kv/" + getSearchPrefix() + "/" + container + "/" + pid;
        }
    }

    private String getSearchPrefix() {
        return kvPrefix + "_raw";
    }

}
