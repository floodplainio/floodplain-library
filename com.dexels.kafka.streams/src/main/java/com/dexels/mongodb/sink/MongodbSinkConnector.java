package com.dexels.mongodb.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongodbSinkConnector implement the Connector interface to send Kafka
 * data to Mongodb
 *
 * @author Andrea Patelli
 */
public class MongodbSinkConnector extends SinkConnector {
    private final static Logger log = LoggerFactory.getLogger(MongodbSinkConnector.class);

    public static final String PORT = "port";
    public static final String HOST = "host";
    public static final String BULK_SIZE = "bulk.size";
    public static final String DATABASE = "mongodb.database";
    public static final String COLLECTIONS = "mongodb.collections";
    public static final String TOPICS = "topics";
    public static final String SINKID = "sinkid";
    public static final String GENERATION = "generation";
    public static final String INSTANCENAME = "instanceName";
    public static final String SINKNAME = "sinkName";
    public static final String TENANT = "tenant";
    public static final String DEPLOYMENT = "deployment";

	
    private String port;
    private String host;
    private String bulkSize;
    private String database;
    private String collections;
    private String topics;
	private String generation;
	private String instanceName;
	private String sinkName;
	private Optional<String> tenant;
	private String deployment;

	public MongodbSinkConnector() {
		System.err.println("mongodb sink connector");
	}
    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a string
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {
        log.info("Parsing configuration: "+map);

        port = map.get(PORT);
        if (port == null || port.isEmpty())
            throw new ConnectException("Missing " + PORT + " config");

        bulkSize = map.get(BULK_SIZE);
        if (bulkSize == null || bulkSize.isEmpty())
            throw new ConnectException("Missing " + BULK_SIZE + " config");

        host = map.get(HOST);
        if (host == null || host.isEmpty())
            throw new ConnectException("Missing " + HOST + " config");

        
        database = map.get(DATABASE);
        collections = map.get(COLLECTIONS);
        topics = map.get(TOPICS);

        if (collections.split(",").length != topics.split(",").length) {
            throw new ConnectException("The number of topics should be the same as the number of collections");
        }

        this.generation = map.get(GENERATION);
        this.instanceName = map.get(INSTANCENAME);
        this.sinkName = map.get(SINKNAME);
        this.tenant = Optional.ofNullable(map.get(TENANT));
        this.deployment = map.get(DEPLOYMENT);

        dumpConfiguration(map, log);
    }

    private void dumpConfiguration(Map<String, String> map, Logger log) {
        log.trace("Starting connector with configuration:");
        for (Map.Entry<String,String> entry : map.entrySet()) {
            log.trace("{}: {}", entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Returns the task implementation for this Connector
     *
     * @return the task class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MongodbSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most maxTasks configurations.
     *
     * @param maxTasks maximum number of task to start
     * @return configurations for tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        List<String> coll = Arrays.asList(collections.split(","));
        int numGroups = Math.min(coll.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(coll, numGroups);
        List<String> topics = Arrays.asList(this.topics.split(","));
        List<List<String>> topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups);
        for (int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(PORT, port);
            config.put(BULK_SIZE, bulkSize);
            config.put(HOST, host);
            config.put(DATABASE, database);
            config.put(GENERATION, generation);
            config.put(INSTANCENAME, instanceName);
            config.put(SINKNAME, sinkName);
            if(tenant.isPresent()) {
                config.put(TENANT, tenant.get());
            }
            config.put(DEPLOYMENT, deployment);
            config.put(COLLECTIONS, String.join(",",dbsGrouped.get(i)));
            config.put(TOPICS, String.join(",",topicsGrouped.get(i)));
            configs.add(config);
        }
        return configs;
    }
    
    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

	@Override
	public ConfigDef config() {
		return new ConfigDef();
	}
}
