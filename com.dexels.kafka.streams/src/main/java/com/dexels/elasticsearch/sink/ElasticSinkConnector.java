package com.dexels.elasticsearch.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

import java.util.*;

/**
 * MongodbSinkConnector implement the Connector interface to send Kafka
 * data to Mongodb
 *
 * @author Andrea Patelli
 */
public class ElasticSinkConnector extends SinkConnector {
	public static final String TOPICS = "topics";
	public static final String INDEXES = "indexes";
	public static final String TYPES = "types";
	public static final String DEPLOYMENT = "deployment";
	public static final String TENANT = "tenant";
	public static final String URL = "url";
	public static final String GENERATION = "generation";
	public static final String BULK_SIZE = "bulk.size";
	public static final String INSTANCENAME = "instanceName";
	public static final String SINKNAME = "sinkName";
	
	private String topics;
	private String url;
	private String types;
	private Optional<String> tenant;
	private String deployment;
	private String indexes;
	private String generation;
	private String instanceName;
	private String sinkName;
	private String bulkSize;


	public ElasticSinkConnector() {
		System.err.println("Elastic sink connector constructed");
	}
    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a string
     */
    @Override
    public String version() {
        return"1";
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {
    	this.topics = map.get(TOPICS);
    	this.indexes = map.get(INDEXES);
    	this.types = map.get(TYPES);
    	this.url = map.get(URL);
    	this.tenant = Optional.ofNullable(map.get(TENANT));
    	this.deployment = map.get(DEPLOYMENT);
        this.generation = map.get(GENERATION);
        this.instanceName = map.get(INSTANCENAME);
        this.sinkName = map.get(SINKNAME);
        bulkSize = map.get(BULK_SIZE);
        if (bulkSize == null || bulkSize.isEmpty())
            throw new ConnectException("Missing " + BULK_SIZE + " config");

    }
    
    /**
     * Returns the task implementation for this Connector
     *
     * @return the task class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSinkTask.class;
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
        List<String> coll = Arrays.asList(indexes.split(","));
        List<String> typeList = Arrays.asList(types.split(","));
        int numGroups = Math.min(coll.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(coll, numGroups);
        List<String> topics = Arrays.asList(this.topics.split(","));
        List<List<String>> topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups);
        for (int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(URL, url);
            config.put(BULK_SIZE, bulkSize);
            config.put(GENERATION, generation);
            config.put(INSTANCENAME, instanceName);
            config.put(SINKNAME, sinkName);
            if(tenant.isPresent()) {
                config.put(TENANT, tenant.get());
            }
            config.put(DEPLOYMENT, deployment);
            config.put(INDEXES, String.join(",",dbsGrouped.get(i)));
            config.put(TOPICS, String.join(",",topicsGrouped.get(i)));
            config.put(TYPES, String.join(",",typeList));
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
