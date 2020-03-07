package com.dexels.mongodb.sink;

import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.mongodb.sink.customcodecs.CoordinateTypeCodecProvider;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.transformer.api.MessageTransformer;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * MongodbSinkTask is a Task that takes records loaded from Kafka and sends them to
 * mongodb.
 *
 * @author Andrea Patelli
 */
public class MongodbSinkTask extends SinkTask {

    @SuppressWarnings("unused")
	private Integer bulkSize;
	private String generation;
	private String instanceName;
	private String sinkName;
	private Optional<String> tenant;
	private String deployment;
	
	private static final AtomicLong updateCount = new AtomicLong();
	private static final Logger logger = LoggerFactory.getLogger(MongodbSinkTask.class);

    private Map<String, MongoCollection<Document>> mapping;

	private MongoClient mongoClient;


    @Override
    public String version() {
        return new MongodbSinkConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
 

        try {
            bulkSize = Integer.parseInt(map.get(MongodbSinkConnector.BULK_SIZE));
        } catch (Exception e) {
            throw new ConnectException("Setting " + MongodbSinkConnector.BULK_SIZE + " should be an integer");
        }
        logger.info("Sink task settings: {}",map);
        
   
        String database = map.get(MongodbSinkConnector.DATABASE);
        String host = map.get(MongodbSinkConnector.HOST);
        String collections = map.get(MongodbSinkConnector.COLLECTIONS);
        String topics = map.get(MongodbSinkConnector.TOPICS);
        
        this.generation = map.get(MongodbSinkConnector.GENERATION);
        this.instanceName = map.get(MongodbSinkConnector.INSTANCENAME);
        this.sinkName = map.get(MongodbSinkConnector.SINKNAME);
        this.tenant = Optional.ofNullable(map.get(MongodbSinkConnector.TENANT));
        this.deployment = map.get(MongodbSinkConnector.DEPLOYMENT);

        List<String> collectionsList = Arrays.asList(collections.split(","));
        List<String> topicsList = Arrays.asList(topics.split(","));
        mongoClient = MongodbSinkTask.createClient(host, map.get(MongodbSinkConnector.PORT));
        MongoDatabase db = mongoClient.getDatabase(database);

        mapping = new HashMap<>();

        for (int i = 0; i < topicsList.size(); i++) {
            String topic = topicsList.get(i);
            String collection = collectionsList.get(i);
            mapping.put(topic, db.getCollection(collection));
        }
    }

	public static MongoClient createClient(String hosts, String portString) {
	    int port;
        try {
            port = Integer.parseInt(portString);
        } catch (Exception e) {
            throw new ConnectException("Setting " + MongodbSinkConnector.PORT + " should be an integer",e);
        }
		List<ServerAddress> l = Arrays.asList(hosts.split(",")).stream().map(host->new ServerAddress(host, port))
		.collect(Collectors.toList());

        // Create the custom registries
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(new CoordinateTypeCodecProvider(true)),
                com.mongodb.MongoClient.getDefaultCodecRegistry());

        MongoClientOptions options = MongoClientOptions.builder().codecRegistry(codecRegistry).build();

        return new MongoClient(l, options);
	}
	
	private Optional<MessageTransformer> transformerForTopic(String topic) {
		return SinkTransformerRegistry.transformerForSink(this.sinkName, new TopologyContext(this.tenant, this.deployment,this.instanceName, this.generation), topic);
	}

    private final TopicElement writeModelFromRecord(SinkRecord record) {
        ReplicationMessage message = (ReplicationMessage) record.value();
		String key = (String)record.key();
		String topic = record.topic();
		
		if(message==null || message.operation().equals(Operation.DELETE)) {
			return new TopicElement(topic, key, new DeleteOneModel<>( Filters.eq("_id", key)));
		}
		
		Optional<MessageTransformer> trans = transformerForTopic(topic);
		if(trans.isPresent()) {
			message = trans.get().apply(CoreOperators.transformerParametersFromTopicName(topic), message);
		}
        Map<String, Object> jsonMap = message.valueMap(true, Collections.emptySet());

        Document newDocument = new Document(jsonMap);

        newDocument.append("_id", key).append("replicated", new Date());

		return new TopicElement(topic,key, new ReplaceOneModel<Document>(
                Filters.eq("_id", key),
                newDocument,
                new UpdateOptions().upsert(true)));
    	
    }
    
    private class TopicElement extends Object {
    	public final String topic;
    	public final String key;
    	public final WriteModel<Document> model;
    	
    	public TopicElement(String topic, String key, WriteModel<Document> model) {
    		this.topic = topic;
    		this.key = key;
    		this.model = model;
    	}
    	
    	public boolean equals(Object e) {
    		if(!(e instanceof TopicElement)) {
    			return false;
    		}
    		return toString().equals(e.toString());
    	}
    	
    	public String toString() {
    		return topic+"-"+key;
    	}

		@Override
		public int hashCode() {
			return toString().hashCode();
		}

    	
    }
    /**
     * Put the records in the sink.
     *
     * @param collection the set of records to send.
     */
	@Override
    public void put(Collection<SinkRecord> collection) {
        List<SinkRecord> records = new ArrayList<>(collection);
        Map<String, Map<String,WriteModel<Document>>> bulks = new HashMap<>();
        records.stream()
        	.map(this::writeModelFromRecord)
        	.forEach(element->{
                if (bulks.get(element.topic) == null) {
                    bulks.put(element.topic, new HashMap<String,WriteModel<Document>>());
                }
                Map<String,WriteModel<Document>> l = bulks.get(element.topic);
                l.put(element.key,element.model);
                
        	});
            BulkWriteOptions op = new BulkWriteOptions();
            op.ordered(false);
            for (String topic : bulks.keySet()) {
                Map<String,WriteModel<Document>> list = bulks.get(topic);
                final String namespace = mapping.get(topic).getNamespace().toString();
				final int size = list.size();
				logOccasionally(namespace, size);
				List<WriteModel<Document>> serialized = list.entrySet().stream().map(e->e.getValue()).collect(Collectors.toList());
                try {
					mapping.get(topic).bulkWrite(serialized, op); 
                } catch (MongoBulkWriteException e) {
                    logger.error("MongoBulkWriteException: ",e);

                    for (BulkWriteError cur : e.getWriteErrors()) {
                        logger.error("Write error on index {}, Category: {}, code: {}, message: {}", cur.getIndex(), cur.getCategory(), cur.getCode(), cur.getMessage() );
                    }
                    
                } catch (Throwable e) {
                	logger.error("Insert error: ", e);
                }
            }
        }

	private void logOccasionally(final String namespace, final int size) {
		long count = updateCount.incrementAndGet();
		if(count % 100 == 0) {
			logger.info("About to write {} documents to {}", size, namespace);
		}
		
	}

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    	//noop
    }

    @Override
    public void stop() {
    	if(mongoClient!=null) {
    		mongoClient.close();
    	}

    }
    

}
