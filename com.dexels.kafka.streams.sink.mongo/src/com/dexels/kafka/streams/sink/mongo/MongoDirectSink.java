package com.dexels.kafka.streams.sink.mongo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

import org.bson.BsonDocument;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.kafka.streams.api.sink.Sink;
import com.dexels.kafka.streams.api.sink.SinkConfiguration;
import com.dexels.replication.api.ReplicationMessage;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.schedulers.Schedulers;

public class MongoDirectSink implements Sink {


	@Override
	public FlowableTransformer<ReplicationMessage, Completable> createTransformer(Map<String, String> attributes,
			Optional<SinkConfiguration> streamConfiguration, String instanceName, Optional<String> tenant,
			String deployment, String generation) {
		 Map<String,String> sinkSettings =  streamConfiguration.get().settings();
		 TopologyContext topologyContext = new TopologyContext(tenant, deployment, instanceName, generation);
		 String resolvedDatabaseName = CoreOperators.topicName(sinkSettings.get("database"),topologyContext);
//		 String url = "mongodb://"+sinkSettings.get("host")+":"+sinkSettings.get("port")+"/"+resolvedDatabaseName;
		 String collection = attributes.get("collection");
		try {
			return createMongoCollection(resolvedDatabaseName, sinkSettings.get("host"),sinkSettings.get("port"),collection);
		} catch (MalformedURLException e) {
			return flow->Flowable.error(e);
		}
	}


//	private Optional<MessageTransformer> transformerForTopic(String topic) {
////		SinkTransformerRegistry.registerTransformerForSink(instanceName, sinkName, tenant, deployment, generation,topicName, transformer.get());
//		return SinkTransformerRegistry.transformerForSink(this.instanceName,this.sinkName, this.tenant, this.deployment, this.generation, topic);
//	}

	public static FlowableTransformer<ReplicationMessage, Completable> createMongoCollection(String database, String host, String port,String collection) throws MalformedURLException {
		 String url = "mongodb://"+host+":"+port+"/"+database;
		 return createMongoCollection(url, collection);
	}
	
	public static FlowableTransformer<ReplicationMessage, Completable> createMongoCollection(String url,String collection) throws MalformedURLException {
		URL u = new URL(url);
		String database = u.getPath();
		MongoCollection<BsonDocument> res = MongoClients.create(url).getDatabase(database).getCollection(collection,BsonDocument.class);
		
		return execute(res);
	}


	private static FlowableTransformer<ReplicationMessage, Completable> execute(MongoCollection<BsonDocument> res) {
		try {
			return flow->flow.buffer(100)
					.observeOn(Schedulers.io())
					.map(e->new MongoSinkTuple(e))
					.map(tuple->{
						return Flowable.fromPublisher(res.bulkWrite(tuple.models))
								.map(e->ImmutableFactory.empty().with("InsertCount", e.getInsertedCount(), "integer"))
								.doOnNext(e->System.err.println("Inserted: "+tuple.models.size()+" elements"))
								.ignoreElements();
					});
			
		} catch (Exception e) {
			return flow->Flowable.error(e);
		}
	}

//    private final MongoSinkTuple writeModelFromRecord(ReplicationMessage message) {
//		String topic = message.source().orElseThrow(()->new UnsupportedOperationException(""));
//		String key = message.combinedKey();
//		
////		if(message==null) {
////			return new TopicElement(topic, key, new DeleteOneModel<>( Filters.eq("_id", key)));
////		} else 
//		if (message.operation().equals(Operation.DELETE)) {
//		    return new MongoSinkTuple(topic, key, new DeleteOneModel<>( Filters.eq("_id", key)));
//		}
//		Optional<MessageTransformer> trans = transformerForTopic(topic);
//		if(trans.isPresent()) {
//			message = trans.get().apply(CoreOperators.transformerParametersFromTopicName(topic), message);
//		}
//        Map<String, Object> jsonMap = message.valueMap(true, Collections.emptySet());
//
//        Document newDocument = new Document(jsonMap);
//
//        newDocument.append("_id", key).append("replicated", new Date());
//
//		return new TopicElement(topic,key, new ReplaceOneModel<Document>(
//                Filters.eq("_id", key),
//                newDocument,
//                new UpdateOptions().upsert(true)));
//    	
//    }
//    
}
