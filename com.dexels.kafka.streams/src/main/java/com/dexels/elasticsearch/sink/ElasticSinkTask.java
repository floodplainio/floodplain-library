package com.dexels.elasticsearch.sink;

import com.dexels.http.reactive.elasticsearch.ElasticInsertTransformer;
import com.dexels.http.reactive.http.HttpInsertTransformer;
import com.dexels.http.reactive.http.JettyClient;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.replication.api.ReplicationMessage;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.eclipse.jetty.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongodbSinkTask is a Task that takes records loaded from Kafka and sends them to
 * mongodb.
 *
 * @author Andrea Patelli
 */
public class ElasticSinkTask extends SinkTask {


	
	private static final Logger logger = LoggerFactory.getLogger(ElasticSinkTask.class);

	private String url;

	private final Map<String,String> typeMapper = new HashMap<>();
	private final Map<String,String> indexMapper = new HashMap<>();


    @Override
    public String version() {
        return new ElasticSinkConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
    	this.url = map.get("url");
        logger.info("Sink task settings: {}",map);

    	String indexes = map.get(ElasticSinkConnector.INDEXES);
    	String topics = map.get(ElasticSinkConnector.TOPICS);
    	String types = map.get(ElasticSinkConnector.TYPES);
        
        String generation = map.get(ElasticSinkConnector.GENERATION);
        String instanceName = map.get(ElasticSinkConnector.INSTANCENAME);
        Optional<String> tenant = Optional.ofNullable(map.get(ElasticSinkConnector.TENANT));
        String deployment = map.get(ElasticSinkConnector.DEPLOYMENT);
        
        TopologyContext topologyContext = new TopologyContext(tenant, deployment, instanceName, generation);

        List<String> topicsList = Arrays.asList(topics.split(","));
        int count = 0;
        System.err.println(" types: "+types);
        System.err.println(" topics: "+topics);
        System.err.println(" indexes: "+indexes);
        
        String[] typeArray = types.split(",");
        for (String type : typeArray) {
        	String topic = topicsList.get(count);
        	System.err.println(" -> Putting topic: "+topic+" to type: "+type);
        	typeMapper.put(topic, type);
			count++;
		}
        System.err.println("TypeMapper after filling: "+typeMapper);
        count=0;
        String[] indexesArray = indexes.split(",");
        for (String index : indexesArray) {
        	String topic = topicsList.get(count);
            String resolvedIndex = CoreOperators.generationalGroup(index,topologyContext);
        	System.err.println(" -> Putting topic: "+topic+" to index: "+resolvedIndex);
        	indexMapper.put(topic, resolvedIndex);
			count++;
		}
        System.err.println("IndexMapper after filling: "+typeMapper);
    }

	/**
     * Put the records in the sink.
     *
     * @param collection the set of records to send.
     */

	@Override
    public void put(Collection<SinkRecord> collection) {
		Flowable<Completable> completables = Flowable.fromIterable(collection)
        	.map(record->((ReplicationMessage)record.value()).withSource(Optional.of(record.topic())))
        	.doOnNext(record->System.err.println(""+record.source()))
        	.compose(ElasticInsertTransformer.elasticSearchInserter(
        			msg->msg.source().orElse("NOTOPIC").toLowerCase(),
        			msg->typeMapper.get(msg.source().orElse("NOTOPIC")).toLowerCase(),
        			100))
        	.compose(HttpInsertTransformer.httpInsert(url,req->req.method(HttpMethod.POST),  "application/x-ndjson",1,1,true))
        	.map(rep->JettyClient.ignoreReply(rep));
		
		completables.flatMapCompletable(e->e).blockingAwait();
	}

	@Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
		// noop
    }

    @Override
    public void stop() {
    	// noop
    }
    

}
