package com.dexels.kafka.streams.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.streams.api.sink.ConnectConfiguration;
import com.dexels.kafka.streams.xml.parser.CaseSensitiveXMLElement;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.kafka.streams.xml.parser.XMLParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class StreamConfiguration {

	
	private final static Logger logger = LoggerFactory.getLogger(StreamConfiguration.class);

	private final String kafkaHosts;
	private final Map<String,ConnectConfiguration> sourceConfigs;
	private final Map<String,ConnectConfiguration> sinkConfigs;
	private final int maxWait;
	private final int maxSize;
	private final int replicationFactor;
	private final Map<String,Object> config;
	private final String connectURL;
	private static final String SOURCETYPE = "source";
	private static final String SINKTYPE = "sink";
	private static final ObjectMapper objectMapper = new ObjectMapper();
	
	public StreamConfiguration(String kafkaHosts, String connectURL, Map<String,ConnectConfiguration> sources, Map<String,ConnectConfiguration> sinks, int maxWait, int maxSize, int replicationFactor) {
		this.kafkaHosts = kafkaHosts;
		this.sourceConfigs = sources;
		this.sinkConfigs = sinks;
		this.config = new HashMap<>();
		this.config.put("bootstrap.servers",kafkaHosts);
		this.config.put("client.id" ,UUID.randomUUID().toString());
		this.connectURL = connectURL;
		this.maxWait = maxWait;
		this.maxSize = maxSize;
		this.replicationFactor = replicationFactor;
	}

//	public static StreamConfiguration parse(InputStream is) {
//		
//	}
	
	public static StreamConfiguration parseConfig(String deployment,InputStream r) {
		try {
			XMLElement xe = new CaseSensitiveXMLElement();
			xe.parseFromStream(r);
			final Optional<StreamConfiguration> found = xe.getChildren()
				.stream()
				.filter(elt->"deployment".equals( elt.getName()))
				.filter(elt->deployment.equals(elt.getStringAttribute("name")))
				.map(elt->toStreamConfig(elt))
				.findFirst();
			if(!found.isPresent()) {
				throw new StreamTopologyException("No configuration found for deployment: "+deployment);
			}
			return found.get();
			
		} catch (XMLParseException | IOException e) {
			throw new StreamTopologyException("Configuration error for deployment: "+deployment,e);
		}
		
	}
	
	private static StreamConfiguration toStreamConfig(XMLElement deploymentXML) {
		String kafka = deploymentXML.getStringAttribute("kafka");
		int maxWait = deploymentXML.getIntAttribute("maxWait", 5000);
		int maxSize = deploymentXML.getIntAttribute("maxSize", 100);
		String connectURL = deploymentXML.getStringAttribute("connect");
		int replicationFactor = deploymentXML.getIntAttribute("replicationFactor", 1);
		Map<String,ConnectConfiguration> sourceConfigs = new HashMap<>();
		Map<String,ConnectConfiguration> sinkConfigs = new HashMap<>();
		for (XMLElement elt : deploymentXML.getChildren()) {
			String[] parts = elt.getName().split("\\.");
			if(parts.length!=2) {
				throw new StreamTopologyException("Connect configurations should be like <type>.source or <type>.sink");
			}
			String connectType = parts[1];
			switch (connectType) {
			case SOURCETYPE:
				sourceConfigs.put(elt.getStringAttribute("name"),new ConnectConfiguration(elt.getName(),elt.getStringAttribute("name"), elt.attributes()));
				break;
			case SINKTYPE:
				sinkConfigs.put(elt.getStringAttribute("name"),new ConnectConfiguration(elt.getName(),elt.getStringAttribute("name"), elt.attributes()));
				break;
			default:
				throw new StreamTopologyException("Connect configurations should be either <type>.source or <type>.sink not: "+connectType);
			}
		}
		
		final StreamConfiguration streamConfiguration = new StreamConfiguration(kafka,connectURL, Collections.unmodifiableMap(sourceConfigs),Collections.unmodifiableMap(sinkConfigs),maxWait,maxSize,replicationFactor);
		return streamConfiguration;
	}


	public Map<String,Object> config() {
		return config;
	}

	public String kafkaHosts() {
		return kafkaHosts;
	}
	
	public String connectURL() {
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

	public Map<String,ConnectConfiguration> sources() {
		return sourceConfigs;
	}
	public Map<String,ConnectConfiguration> sinks() {
		return sinkConfigs;
	}
	
	public Optional<ConnectConfiguration> source(String name) {
		return Optional.ofNullable(this.sourceConfigs.get(name));
	}
	public Optional<ConnectConfiguration> sink(String name) {
		return Optional.ofNullable(this.sinkConfigs.get(name));
	}

	public void startSink(TopologyContext context, ConnectConfiguration config, boolean force) throws IOException {
//		ConnectConfiguration config = sink(name).orElseThrow(()->new StreamTopologyException("Can't start sink. Unknown sink: "+name));
		String generatedName = CoreOperators.topicName(config.name(), context);

		List<String> current = existingConnectors();
		if(current.contains(generatedName)) {
			if(force) {
				logger.warn("Force enabled, deleting old");
				deleteConnector(generatedName);
			} else {
				logger.warn("Connector: {} already present, ignoring",generatedName);
				return;
			}
		}
		String connector = config.settings().get("connector.class");
		if(connector==null) {
			logger.warn("No connector for sink, so ignoring for now");
			return;
		}
		ObjectNode node = objectMapper.createObjectNode();
		node.put("name", generatedName);
		ObjectNode configNode = objectMapper.createObjectNode();
		node.set("config", configNode);
		config.settings().forEach((k,v)->{
			configNode.put(k, v);
		});
		// override name to match general name
		configNode.put("name", generatedName);
		configNode.put("database.server.name", generatedName);
		String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
		System.err.println(">> "+jsonString);
		postToHttp(jsonString);
		
//		HttpRequest request = HttpRequest.newBuilder()
//                .POST(HttpRequest.BodyPublishers.ofString(json))
//                .uri(URI.create("https://httpbin.org/post"))
//                .setHeader("User-Agent", "Java 11 HttpClient Bot") // add request header
//                .header("Content-Type", "application/json")
//                .build();
	}
	
	private List<String> existingConnectors() throws IOException {
		URL url = new URL(this.connectURL);
		ArrayNode an = (ArrayNode) objectMapper.readTree(url.openStream());
		List<String> result = new ArrayList<>();
		an.forEach(j->result.add(j.asText()));
		return Collections.unmodifiableList(result);
	}
	
	
	private void deleteConnector(String name) throws IOException {
		URL url = new URL(this.connectURL+"/"+name);
		HttpURLConnection con = (HttpURLConnection)url.openConnection();
		con.setRequestMethod("DELETE");
		int code = con.getResponseCode();
		logger.info("Delete result: {}",code);
}
	
	// TODO replace with Java 11 client when we can go to graal 19.3
	private void postToHttp(String jsonString) throws ProtocolException, IOException {
		URL url = new URL(this.connectURL);
		logger.info("Posting to: {}",this.connectURL);
		HttpURLConnection con = (HttpURLConnection)url.openConnection();
		
//		-H "Accept:application/json" -H "Content-Type:application/json"
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/json");
		con.setRequestProperty("Accept", "application/json");
		con.setDoOutput(true);
		try(OutputStream os = con.getOutputStream()) {
		    byte[] input = jsonString.getBytes("utf-8");
		    os.write(input, 0, input.length);           
		}
		logger.info("Result code: {} and message: {}",con.getResponseCode(),con.getResponseMessage());
		
		try(BufferedReader br = new BufferedReader(
				  new InputStreamReader(con.getInputStream(), "utf-8"))) {
				    StringBuilder response = new StringBuilder();
				    String responseLine = null;
				    while ((responseLine = br.readLine()) != null) {
				        response.append(responseLine.trim());
				    }
				    System.out.println(response.toString());
				}
	}
}
