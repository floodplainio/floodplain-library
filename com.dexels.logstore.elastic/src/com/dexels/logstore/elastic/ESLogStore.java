package com.dexels.logstore.elastic;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.logstore.LogLevelFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component(name="navajo.logstore.elasticsearch",configurationPolicy=ConfigurationPolicy.REQUIRE,property={"event.topics=logserver/logentry"},immediate=true)
public class ESLogStore implements EventHandler {
    private final static Logger logger = LoggerFactory.getLogger(ESLogStore.class);

    private static final int MAX_BACKLOG_SIZE = 50000;

    private ThreadPoolExecutor tpe;
    
    private SimpleDateFormat df;
    private SimpleDateFormat df_es;
    private CloseableHttpClient httpclient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, String> logLevels;


    private String url;
    private String index;
    private String defaultLogLevel;
    private String sourceContainer;
    private String sourceHost;

    @Activate
    public void activate(Map<String, Object> settings) {
        logger.info("Activating elasticsearch logstore");
        
        tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(25);
        
        Map<String,String> env = System.getenv();
        
        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        df_es = new SimpleDateFormat("yyyy.MM.dd");
        df_es.setTimeZone(TimeZone.getTimeZone("UTC"));
        httpclient = HttpClients.createDefault();
        logLevels = new HashMap<>();

        this.url = (String) settings.get("url");
        this.index = (String) settings.get("index");
        this.defaultLogLevel = (String) settings.get("defaultLogLevel");
        
        this.sourceContainer = getenv(env, "CONTAINERNAME");
        this.sourceHost = getenv(env, "HOSTNAME");

        String logLevels = (String) settings.get("logLevels");
        String[] splitted = logLevels.split(",");
        for (String pair : splitted) {
            String[] pairSplit = pair.split(":");
            if (pairSplit.length != 2) {
                logger.warn("Ignoring loglevel pair {} due to wrong format", pair);
                continue;
            }
            String className = pairSplit[0];
            String level = pairSplit[1];
            this.logLevels.put(className, level);
        }
    }
    
    @Deactivate
    public void deactivate() {
        tpe.shutdown();
        
    }

    @Override
    public void handleEvent(Event event) {
        final String jsonEvent = (String) event.getProperty("logEntry");

        if (tpe.getQueue().size() > MAX_BACKLOG_SIZE) {
            //ignore
            logger.warn("max backlog exceeded - dropping event");
            return;
        }
        tpe.execute(new Runnable() {
            @Override
            public void run() {
                if (jsonEvent.charAt(0) == '[') {
                    try {
                        @SuppressWarnings("unchecked")
                        ArrayList<String> list = objectMapper.readValue(jsonEvent, ArrayList.class);
                        for (String objNode : list) {

                            processObjectNode(objNode);
                        }
                    } catch (Throwable e) {
                        logger.error("Error on parsing array event", e);
                    }

                } else {
                    processObjectNode(jsonEvent);
                }
            }
        });
    }

    private void processObjectNode(String stringObjectNode)  {
       try {
           ObjectNode mm = (ObjectNode) objectMapper.readTree(stringObjectNode);
           
           String logLevel = mm.get("level").textValue();
           String className = mm.get("categoryName").textValue();

           // Check log level. If we have an override for this class name,
           // use that. Otherwise use default
           String logLevelToCheck = defaultLogLevel;
           if (logLevels.containsKey(className)) {
               logLevelToCheck = logLevels.get(className);
           }
           if (!LogLevelFilter.isHigherOrEqual(logLevel, logLevelToCheck)) {
               return;
           }
           // Replace any fields with a key in them
           replaceDots(mm);
           
           // If MDC contains a sessiontoken, also include the seperate fields
           if (mm.has("mdc") && mm.get("mdc").has("sessionToken")) {
               String sessionToken = mm.get("mdc").get("sessionToken").textValue();
               String[] splitted = sessionToken.split("\\|");
               if (splitted.length == 4) {
                   mm.put("username", splitted[0]);
                   mm.put("ip", splitted[1]);
                   mm.put("hostname", splitted[2]);
                   mm.put("sessionstarted", df.format(new Date(Long.valueOf(splitted[3]))));
               }
           }

           mm.put("source_container", sourceContainer);
           mm.put("source_host", sourceHost);

           Long timestamp = mm.get("timestamp").asLong();
           Date eventDate = new Date(timestamp);
           String indexDate = df_es.format(eventDate);
           mm.put("@timestamp", df.format(eventDate));
           mm.remove("timestamp");

           String type = "log";
           if (mm.has("content")) {
               type = "stats";
           }
           
          

           URI uri = assembleURI(mm, indexDate, type);
           postJSON(mm, uri);
       } catch (URISyntaxException e) {
           logger.error("URISyntaxException: ", e);
       } catch (IOException e) {
           logger.error("Error on writing json to ES: ", e);
       }
    }

    private void replaceDots(ObjectNode mm) {
        Iterator<Entry<String, JsonNode>> it = mm.fields();
        Set<String> fieldsWithDots = new HashSet<>();
        Set<String> nestedFields = new HashSet<>();
        
        while (it.hasNext()) {
            Entry<String, JsonNode> entry = it.next();
            if (entry.getValue().size() > 0)  {
                nestedFields.add(entry.getKey());
            }
            if (entry.getKey().contains(".")) {
                fieldsWithDots.add(entry.getKey());
            }
        }
        for (String key : nestedFields) {
            replaceDots((ObjectNode) mm.get(key));
        }
        for (String key : fieldsWithDots) {
            String newKey = key.replace(".", "_");
            JsonNode value = mm.get(key);
            mm.set(newKey, value);
            mm.remove(key);
        }
    }

    private URI assembleURI(ObjectNode node, String indexDate, String type) throws URISyntaxException {
        StringBuilder sb = new StringBuilder(url);
        if (!url.endsWith("/")) {
            sb.append("/");
        }
        sb.append(index);
        sb.append("-");
        sb.append(indexDate);
        sb.append("/");

        sb.append(type);
        sb.append("/");

        return new URI(sb.toString());
    }

    private void postJSON(ObjectNode node, URI uri) throws IOException {

        HttpPost httpPut = new HttpPost(uri);
        byte[] requestBytes = objectMapper.writer().withDefaultPrettyPrinter().writeValueAsBytes(node);

        HttpEntity he = new ByteArrayEntity(requestBytes);
        httpPut.setEntity(he);
        CloseableHttpResponse response1 = httpclient.execute(httpPut);
        logger.debug("ES put result: {}", EntityUtils.toString(response1.getEntity()));
        response1.close();
    }
    
    private final String getenv(Map<String, String> env, String key) {
        String result = env.get(key);
        if(result!=null) {
            return result;
        }
        return System.getProperty(key);
    }
       
}
