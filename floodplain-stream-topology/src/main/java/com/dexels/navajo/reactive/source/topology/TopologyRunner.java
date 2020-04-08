package com.dexels.navajo.reactive.source.topology;

import com.dexels.kafka.converter.ReplicationMessageConverter;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sink.ConnectConfiguration;
import com.dexels.kafka.streams.api.sink.ConnectType;
import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor;
import com.dexels.kafka.streams.remotejoin.TopologyConstructor.ConnectorTopicTuple;
import com.dexels.kafka.streams.remotejoin.TopologyDefinitionException;
import com.dexels.kafka.streams.tools.KafkaUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class TopologyRunner {


    private final static Logger logger = LoggerFactory.getLogger(TopologyRunner.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    //	private final TopologyContext topologyContext;
    private final TopologyConstructor topologyConstructor;


    private final Map<String, Object> baseSettings;
    private final Properties props;

    private final StreamConfiguration streamConfiguration;
    private boolean offline;

    private class MaterializedConnector {
        public Optional<List<Map<String, Object>>> list;
        public Optional<Map<String, Object>> single;

        public MaterializedConnector(List<Map<String, Object>> list) {
            this.list = Optional.of(list);
            this.single = Optional.empty();
        }

        public MaterializedConnector(Map<String, Object> single) {
            this.list = Optional.empty();
            this.single = Optional.of(single);
        }

    }

    public TopologyRunner(String storagePath, String applicationId, StreamConfiguration streamConfiguration, boolean offline) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("key.converter", ReplicationMessageConverter.class.getName());
        settings.put("key.converter.schemas.enable", false);
        settings.put("value.converter", ReplicationMessageConverter.class.getName());
        settings.put("value.converter.schemas.enable", false);
        this.offline = offline;
        baseSettings = Collections.unmodifiableMap(settings);
        this.streamConfiguration = streamConfiguration;
        props = StreamInstance.createProperties(applicationId, streamConfiguration.kafkaHosts(), storagePath);
        this.topologyConstructor = new TopologyConstructor(offline ? Optional.empty() : Optional.of(AdminClient.create(props)));
    }

    public KafkaStreams runTopology(Topology topology) throws InterruptedException, IOException {
        KafkaStreams stream = new KafkaStreams(topology, props);
        stream.setUncaughtExceptionHandler((thread, exception) -> {
            logger.error("Error in streams: ", exception);
            stream.close();
        });
        stream.setStateListener((oldState, newState) -> {
            logger.info("State moving from {} to {}", oldState, newState);
        });
        if (this.offline) {
            logger.warn("In offline mode, KafkaStreams instance won't be started");
        } else {
            stream.start();
        }
        return stream;
    }

    public TopologyConstructor topologyConstructor() {
        return topologyConstructor;
    }

    public static void startConnector(TopologyContext topologyContext, URL connectURL, String connectorName, boolean force, Map<String, Object> parameters) throws IOException {
        String generatedName = CoreOperators.topicName(connectorName, topologyContext);

        List<String> current = existingConnectors(connectURL);
        if (current.contains(generatedName)) {
            if (force) {
                logger.warn("Force enabled, deleting old");
                deleteConnector(generatedName, connectURL);
            } else {
                logger.warn("Connector: {} already present, ignoring", generatedName);
                return;
            }
        }
        String connector = (String) parameters.get("connector.class");
        if (connector == null) {
            throw new TopologyDefinitionException("Error creating connector message for connector: " + connectorName + " it has no connector.class setting");
        }
        ObjectNode node = objectMapper.createObjectNode();
        node.put("name", generatedName);
        ObjectNode configNode = objectMapper.createObjectNode();
        node.set("config", configNode);
        parameters.forEach((k, v) -> {
            if (v instanceof String) {
                configNode.put(k, (String) v);
            } else if (v instanceof Integer) {
                configNode.put(k, (Integer) v);
            } else if (v instanceof Long) {
                configNode.put(k, (Long) v);
            } else if (v instanceof Float) {
                configNode.put(k, (Float) v);
            } else if (v instanceof Double) {
                configNode.put(k, (Double) v);
            } else if (v instanceof Boolean) {
                configNode.put(k, (Boolean) v);
            }
        });
        // override name to match general name
        configNode.put("name", generatedName);
        // TODO this seems debezium specific
        configNode.put("database.server.name", generatedName);
        String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        logger.info(">> {}", jsonString);
        postToHttp(connectURL, jsonString);
    }

    private static List<String> existingConnectors(URL url) throws IOException {
        ArrayNode an = (ArrayNode) objectMapper.readTree(url.openStream());
        List<String> result = new ArrayList<>();
        an.forEach(j -> result.add(j.asText()));
        return Collections.unmodifiableList(result);
    }


    private static void deleteConnector(String name, URL connectURL) throws IOException {
        URL url = new URL(connectURL + "/" + name);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("DELETE");
        int code = con.getResponseCode();
        logger.info("Delete result: {}", code);
    }

    // TODO replace with Java 11 client when we can go to graal 19.3
    private static void postToHttp(URL url, String jsonString) throws ProtocolException, IOException {
//		URL url = new URL(this.connectURL);
        logger.info("Posting to: {}", url);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

//		-H "Accept:application/json" -H "Content-Type:application/json"
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = jsonString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
        logger.info("Result code: {} and message: {}", con.getResponseCode(), con.getResponseMessage());

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), "utf-8"))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            System.out.println(response.toString());
        }
    }


    public void materializeConnectors(TopologyContext topologyContext, boolean force) throws IOException {
        if (!streamConfiguration.connectURL().isPresent()) {
            logger.warn("No connectURL present, so not materializing anything");
            return;
        }
        Set<String> topics = new HashSet<>();
        for (Entry<String, List<ConnectorTopicTuple>> e : topologyConstructor.connectorAssociations.entrySet()) {
            for (ConnectorTopicTuple tuple : e.getValue()) {
                topics.add(tuple.topicName);
            }
        }
        topologyConstructor.adminClient.ifPresent(admin -> {
            KafkaUtils.ensureExistSync(topologyConstructor.adminClient, topics, CoreOperators.topicPartitionCount(), CoreOperators.topicReplicationCount());
        });

        for (Entry<String, List<ConnectorTopicTuple>> e : topologyConstructor.connectorAssociations.entrySet()) {
            List<ConnectorTopicTuple> list = e.getValue();
            Optional<ConnectConfiguration> cc = streamConfiguration.connector(e.getKey());
            if (!cc.isPresent()) {
                throw new TopologyDefinitionException("Missing sink resource named: " + e.getKey());
            }
            MaterializedConnector parsed = parseConnector(list, cc.get());
            if (parsed.list.isPresent()) {
                int connectorCount = 0;
                for (Map<String, Object> element : parsed.list.get()) {
                    Map<String, Object> processed = resolveGenerationsForSettings(topologyContext, element);
                    assembleConnector(topologyContext, cc.get(), processed, streamConfiguration.connectURL().get(), force, Optional.of(connectorCount));
                    connectorCount++;
                }
            } else {
                Map<String, Object> element = parsed.single.get();
                Map<String, Object> processed = resolveGenerationsForSettings(topologyContext, element);
                assembleConnector(topologyContext, cc.get(), processed, streamConfiguration.connectURL().get(), force, Optional.empty());
            }
        }
    }


    private Map<String, Object> resolveGenerationsForSettings(TopologyContext topologyContext, Map<String, Object> input) {
        Map<String, Object> result = new HashMap<>();
        input.entrySet().forEach(element -> {
            Object value = element.getValue();
            if (value instanceof String) {
                result.put(element.getKey(), CoreOperators.resolveGenerations((String) element.getValue(), topologyContext));
            } else {
                result.put(element.getKey(), element.getValue());
            }
        });
        return result;
    }

    private MaterializedConnector parseConnector(List<ConnectorTopicTuple> tuples, ConnectConfiguration connectorConfig) {
        String clazz = connectorConfig.settings().get("connector.class");
        switch (clazz) {
            case "io.debezium.connector.postgresql.PostgresConnector":
                List<String> whitelist = tuples.stream().map(e -> e.sinkParameters.get("schema") + "." + e.sinkParameters.get("table")).collect(Collectors.toList());
                Set<String> unique = new HashSet<>(whitelist);
                String whitelistFormat = unique.stream().collect(Collectors.joining(","));
                Map<String, Object> settings = new HashMap<>(connectorConfig.settings());
//			settings.putAll(this.baseSettings);
                // TODO if 'resource' is still in the map, I should remove it, right?
                settings.put("table.whitelist", whitelistFormat);
                return new MaterializedConnector(settings);
            case "com.mongodb.kafka.connect.MongoSinkConnector":
                List<Map<String, Object>> result = new ArrayList<>();
                for (ConnectorTopicTuple connectorTopicTuple : tuples) {
                    Map<String, Object> cSettings = new HashMap<>(connectorConfig.settings());
                    cSettings.putAll(connectorTopicTuple.sinkParameters);
                    cSettings.putAll(this.baseSettings);
                    cSettings.put("topics", connectorTopicTuple.topicName);
                    cSettings.put("tasks.max", "1");
                    cSettings.put("document.id.strategy", "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy");
                    logger.info("Settings: {}", cSettings);
                    result.add(cSettings);
                }
                return new MaterializedConnector(result);
//			{
//				  "name": "HttpSink",
//				  "config": {
//				    "key.converter":"org.apache.kafka.connect.storage.StringConverter",
//				    "key.converter.schemas.enable": false,
//				    "headers": "Accept:application/json|Content-Type:application/json"
//				  }
//				}
            case "uk.co.threefi.connect.http.HttpSinkConnector":
                List<Map<String, Object>> httpresult = new ArrayList<>();
                for (ConnectorTopicTuple connectorTopicTuple : tuples) {
                    Map<String, Object> cSettings = new HashMap<>(connectorConfig.settings());
                    cSettings.putAll(this.baseSettings);
                    cSettings.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
                    cSettings.put("key.converter.schemas.enable", false);
                    cSettings.put("value.converter", "com.dexels.kafka.converter.ReplicationMessageConverter");
                    cSettings.put("value.converter.schemas.enable", false);
                    cSettings.put("topics", connectorTopicTuple.topicName);
                    cSettings.put("tasks.max", "1");
                    cSettings.putAll(connectorTopicTuple.sinkParameters);
                    logger.info("Settings: {}", cSettings);
                    httpresult.add(cSettings);
                }
                return new MaterializedConnector(httpresult);
            case "io.floodplain.sink.SheetSinkConnector":
                List<Map<String, Object>> matList = new ArrayList<>();
//			Map<String,String> dSettings = new HashMap<>(connectorConfig.settings());
                for (ConnectorTopicTuple tuple : tuples) {
                    Map<String, Object> cSettings = new HashMap<>(connectorConfig.settings());
//				cSettings.putAll(baseSettings);
                    cSettings.put("topics", tuple.topicName);
                    cSettings.put("tasks.max", "1");
//				cSettings.put("schemas.enable",false);
                    cSettings.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
                    cSettings.put("key.converter.schemas.enable", false);
                    cSettings.put("value.converter.schemas.enable", false);
                    cSettings.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
                    cSettings.putAll(tuple.sinkParameters);
                    matList.add(cSettings);
                }
                return new MaterializedConnector(matList);
            default:
                throw new UnsupportedOperationException("Unknown connector class: " + clazz);
        }
    }

    private void assembleConnector(TopologyContext topologyContext, ConnectConfiguration cc, Map<String, Object> parameters, URL connectURL, boolean force, Optional<Integer> connectorCount) throws IOException {
        Map<String, Object> result = new HashMap<>();
        result.putAll(parameters);
        String connectorName = connectorCount.map(c -> cc.name() + "_" + c).orElse(cc.name());
        if (this.offline) {
            logger.warn("No connectors will be started in offline mode");
        } else {
            startConnector(topologyContext, connectURL, connectorName, force, result);
        }
    }

}
