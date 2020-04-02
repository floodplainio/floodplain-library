package com.dexels.kafka.streams.api;

import com.dexels.kafka.streams.api.sink.ConnectConfiguration;
import com.dexels.kafka.streams.api.sink.ConnectType;
import com.dexels.kafka.streams.xml.parser.CaseSensitiveXMLElement;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.kafka.streams.xml.parser.XMLParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;


public class StreamConfiguration {


    private final static Logger logger = LoggerFactory.getLogger(StreamConfiguration.class);

    private final String kafkaHosts;
    //	private final Map<String,ConnectConfiguration> sourceConfigs;
//	private final Map<String,ConnectConfiguration> sinkConfigs;
    private final Map<String, ConnectConfiguration> connectorConfigs;
    private final int maxWait;
    private final int maxSize;
    private final int replicationFactor;
    private final Map<String, Object> config;
    private final Optional<URL> connectURL;
    private static final String SOURCETYPE = "source";
    private static final String SINKTYPE = "sink";
    private final HttpClient client;

    public StreamConfiguration(String kafkaHosts, Optional<String> connectURL, Map<String, ConnectConfiguration> connectors, int maxWait, int maxSize, int replicationFactor) throws IOException {
        this.kafkaHosts = kafkaHosts;
//		this.sourceConfigs = sources;
//		this.sinkConfigs = sinks;
        this.connectorConfigs = connectors;
        this.config = new HashMap<>();
        this.config.put("bootstrap.servers", kafkaHosts);
        this.config.put("client.id", UUID.randomUUID().toString());
        this.maxWait = maxWait;
        this.maxSize = maxSize;
        this.replicationFactor = replicationFactor;
        this.connectURL = connectURL.isPresent() ? Optional.of(new URL(connectURL.get())) : Optional.empty();
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

//	public static StreamConfiguration parse(InputStream is) {
//		
//	}


    public void verifyConnectURL(int retries, Duration retryDelay) throws URISyntaxException, InterruptedException {
        if (!connectURL.isPresent()) {
            return;
        }
        URI uri = connectURL.get().toURI();
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(uri)
                .build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                Thread.sleep(retryDelay.toMillis());
                verifyConnectURL(retries - 1, retryDelay);
            }
        } catch (IOException e) {
            Thread.sleep(retryDelay.toMillis());
            verifyConnectURL(retries - 1, retryDelay);
            e.printStackTrace();
        }
    }


    public static StreamConfiguration parseConfig(String deployment, InputStream r) {
        try {
            XMLElement xe = new CaseSensitiveXMLElement();
            xe.parseFromStream(r);
            final Optional<StreamConfiguration> found = xe.getChildren()
                    .stream()
                    .filter(elt -> "deployment".equals(elt.getName()))
                    .filter(elt -> deployment.equals(elt.getStringAttribute("name")))
                    .map(elt -> toStreamConfig(elt))
                    .findFirst();
            if (!found.isPresent()) {
                throw new StreamTopologyException("No configuration found for deployment: " + deployment);
            }
            return found.get();

        } catch (XMLParseException | IOException e) {
            throw new StreamTopologyException("Configuration error for deployment: " + deployment, e);
        }

    }

    private static StreamConfiguration toStreamConfig(XMLElement deploymentXML) {
        String kafka = deploymentXML.getStringAttribute("kafka");
        int maxWait = deploymentXML.getIntAttribute("maxWait", 5000);
        int maxSize = deploymentXML.getIntAttribute("maxSize", 100);
        Optional<String> connectURL = Optional.ofNullable(deploymentXML.getStringAttribute("connect"));
        int replicationFactor = deploymentXML.getIntAttribute("replicationFactor", 1);
        Map<String, ConnectConfiguration> connectorConfigs = new HashMap<>();
        for (XMLElement elt : deploymentXML.getChildren()) {
            String[] parts = elt.getName().split("\\.");
            if (parts.length != 2) {
                throw new StreamTopologyException("Connect configurations should be like <type>.source or <type>.sink");
            }
            String connectType = parts[1];
            switch (connectType) {
                case SOURCETYPE:
                    connectorConfigs.put(elt.getStringAttribute("name"), new ConnectConfiguration(ConnectType.SOURCE, elt.getStringAttribute("name"), elt.attributes()));
                    break;
                case SINKTYPE:
                    connectorConfigs.put(elt.getStringAttribute("name"), new ConnectConfiguration(ConnectType.SINK, elt.getStringAttribute("name"), elt.attributes()));
                    break;
                default:
                    throw new StreamTopologyException("Connect configurations should be either <type>.source or <type>.sink not: " + connectType);
            }
        }

        try {
            return new StreamConfiguration(kafka, connectURL, Collections.unmodifiableMap(connectorConfigs), maxWait, maxSize, replicationFactor);
        } catch (IOException e) {
            throw new StreamTopologyException("Malformed connectURL in config. It is optional, so remove if not needed.", e);
        }
    }


    public Map<String, Object> config() {
        return config;
    }

    public String kafkaHosts() {
        return kafkaHosts;
    }

    public Optional<URL> connectURL() {
        return connectURL;
    }

    public int kafkaSubscribeMaxWait() {
        return maxWait;
    }

    public int kafkaSubscribeMaxSize() {
        return maxSize;
    }

    public int kafkaReplicationFactor() {
        return replicationFactor;
    }

    public Map<String, ConnectConfiguration> connectors() {
        return this.connectorConfigs;
    }

    public Optional<ConnectConfiguration> connector(String name) {
        return Optional.ofNullable(this.connectorConfigs.get(name));
    }

}
