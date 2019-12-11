package com.dexels.neo4j;


import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.pubsub.rx2.api.TopicSubscriber;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;


@Component(enabled=true)
@SuppressWarnings("unused")

public class Example {
	
	private TopicSubscriber subscriber;
	private final AtomicLong relationCount = new AtomicLong();
	private final List<Disposable> subscribers = new ArrayList<>();
	
	private static final Logger logger = LoggerFactory.getLogger(Example.class);

	
	@Activate
	public void activate() {
		Driver driver = GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic("neo4j","neo4jpw"));
		replicateTenant(driver,"KNBSB","develop","g21");
		if(true) {
			return;
		}
	}

	@Deactivate
	public void deactivate() {
		for (Disposable subscription : subscribers) {
			subscription.dispose();
		}
	}
	private void replicateTenant(Driver driver,String tenant, String deployment, String group) {
		subscribers.add(insertRelationshipStream(driver, tenant, deployment, "sportlinkkernel-ORGANIZATIONPERSON",group,"Person","Club","personid","organizationid","memberOf","organizationpersonid"));
		subscribers.add(insertRelationshipStream(driver, tenant, deployment, "sportlinkkernel-TEAMPERSON",group,"Person","Team","personid","teamid","teamMemberOf","teampersonid"));
		subscribers.add(insertRelationshipStream(driver, tenant, deployment, "sportlinkkernel-MATCHTEAMMEMBER",group,"Person","Match","personid","matchid","playedInMatch","matchteammemberid"));
		
	}

	private Disposable insertNodeStream(Driver driver, String tenant, String deployment, String name, String group, String label,Function<ReplicationMessage,Optional<String>> displayNameExtractor) {
		final String topic = tenant+"-"+deployment+"-"+name;
		logger.info("Registering topic: {} with group: {}",topic,group);
		return Observable.fromPublisher(subscriber.subscribe(Arrays.asList(topic), group,Optional.empty(), true,()->{}))
			.subscribeOn(Schedulers.io())
			.flatMap(Observable::fromIterable)
			.map(pubsub->ReplicationFactory.getInstance().parseBytes(pubsub))
			.buffer(10,TimeUnit.SECONDS, 100)
			.subscribe(msg->bulkInsertNode(label, driver, msg,displayNameExtractor));
	}

	private Disposable insertRelationshipStream(Driver driver, String tenant, String deployment, String name, String group,  String sourceLabel, String targetLabel, String sourceIdProperty, String targetIdProperty, String relationshipLabel, String relationshipIdProperty) {
		final String topic = tenant+"-"+deployment+"-"+name;
		logger.info("Registering topic: {} with group: {}",topic,group);
		return Observable.fromPublisher(subscriber.subscribe(Arrays.asList(topic), group,Optional.empty(), true,()->{}))
			.subscribeOn(Schedulers.io())
			.flatMap(e->Observable.fromIterable(e))
			.map(pubsub->ReplicationFactory.getInstance().parseBytes(pubsub))
			.buffer(10,TimeUnit.SECONDS, 100)
			.subscribe(msg->bulkInsertRelation(driver,sourceLabel,targetLabel,sourceIdProperty,targetIdProperty,relationshipLabel,relationshipIdProperty, msg));
	}
	
	private void uploadStanding(String path, ReplicationMessage msg) {
		String key = msg.queueKey();
		String url = "https://test-a1c56.firebaseio.com/"+path+"/"+key+".json";
		String content = msg.toFlatString(ReplicationFactory.getInstance());

		try {
			URL u = new URL(url);
			HttpURLConnection connection = (HttpURLConnection) u.openConnection();
			connection.setRequestMethod("PUT");
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setRequestProperty("Accept", "application/json");
			OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
			osw.write(content);
			osw.flush();
			osw.close();
			logger.info("result: {}",connection.getResponseCode());
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}

	@Reference
	public void setTopicSubscriber(TopicSubscriber subscriber) {
		this.subscriber = subscriber;
	}
	
	private String cypherEncode(Object o, String type) {
		if("string".equals(type)) {
			return "\""+((String)o).replaceAll("\"", "'")+"\"";
		}
		if("date".equals(type)) {
			Date d = (Date)o;
			return ""+d.getTime();
		}
		return ""+o;
	}
	
	private void ensureNodeExists(Session session, String label, Object id) {
		Map<String,Object> params = new HashMap<>();
		params.put("id", id);
		String query = "MERGE (c:%s {id:$id}) ON CREATE SET c.created = timestamp(),c.id =$id";
		String assembledQuery = String.format(query,label, id);

//		System.err.println("Params: "+params+" query: "+assembledQuery);
		StatementResult sr = session.run(assembledQuery,params);
		sr.consume();
	}
	
	private void insertRelationMessage(Driver driver, String sourceLabel, String targetLabel, String sourceIdProperty, String targetIdProperty, String relationshipLabel, String relationshipIdProperty,List<ReplicationMessage> m) {
		try(Session session = driver.session()) {
//			for (ReplicationMessage replicationMessage : m) {
//				String sourceId = (String)replicationMessage.columnValue(sourceIdProperty);
//				String targetId = (String)replicationMessage.columnValue(targetIdProperty);
//				String relationshipId = (String)replicationMessage.columnValue(targetIdProperty);
				insertRelation(session, sourceLabel, targetLabel, relationshipLabel, m);
//			}
		}
	}

	private void bulkInsertRelation(Driver driver, String sourceLabel, String targetLabel, String sourceIdProperty, String targetIdProperty, String relationshipLabel, String relationshipIdProperty, List<ReplicationMessage> m) {
		if(m.size()==0) {
			return;
		}
		System.err.println("Bulk inserting "+m.size()+" relations.");
		try(Session session = driver.session()) {
			insertRelation(session, sourceLabel, targetLabel,relationshipLabel, m);
		}
		m.get(m.size()-1).commit();
	}	
	
	private void insertRelation(Session session, String sourceLabel, String targetLabel,String relationshipLabel,List<ReplicationMessage> m) {
		long l = this.relationCount.incrementAndGet();
		long started = System.currentTimeMillis();
//		ensureNodeExists(session, sourceLabel, m.columnValue(sourceIdProperty));
//		long ensureSource = System.currentTimeMillis();
//		ensureNodeExists(session, targetLabel, m.columnValue(targetIdProperty));
//		long ensureTarget = System.currentTimeMillis();
//		
//		Map<String,Object> v = m.message().flatValueMap("", (key,type,value)->{
//			if("string".equals(type)) {
//				String val = (String)value;
//				return val;
//			}
//			if("date".equals(type)) {
//				Date d = (Date)value;
//				return d.getTime();
//			}
//			return value;
//		});
		
		List<Map<String,Object>> parameterList = m.stream().map(msg->msg.message().flatValueMap("", (key,type,value)->{
			if("string".equals(type)) {
				String val = (String)value;
				return val;
			}
			if("date".equals(type)) {
				Date d = (Date)value;
				return d.getTime();
			}
			return value;
		})).collect(Collectors.toList());
		
//		Map<String,Object> v = m.flatValueMap("",(key,type,value)->{
//			if("string".equals(type)) {
//				String val = (String)value;
//				return val;
//			}
//			if("date".equals(type)) {
//				Date d = (Date)value;
//				return d.getTime();
//			}
//			return value;
//		});
//		String relationshipId = ""+m.columnValue(relationshipIdProperty);
		
//		String data = "{id: \""+m.queueKey()+"\", " + v.keySet().stream().map(s->s+": "+cypherEncode(m.columnValue(s), m.columnType(s))).collect(Collectors.joining(","))+"}";
//		String query = "MATCH (n:%s {id:$sourceId}),( c:%s {id:$targetId}) MERGE (n)-[r:%s {id:$relationshipId}]->(c) ON CREATE SET r = %s ON MATCH SET r = %s";		

//	    WITH  {batch_list} AS batch_list
//	    UNWIND batch_list AS rows
//	    WITH rows, toInteger(rows[0]) AS userid
//	    MATCH (u:User {userId: userid})
//	    MERGE (u)-[r:HAS_DAILY_CHARGE]->(n:DailyCharge {userId: toInteger(rows[0])})
//	    ON CREATE SET n.amountUSD = toFloat(rows[1]), n.createdDate = toFloat(rows[2])	
		

	    String query = "WITH {list} AS list UNWIND list AS rows MATCH (n:%s {id:$sourceId}),( c:%s {id:$targetId}) MERGE (n)-[r:%s {id:$relationshipId}]->(c) ON CREATE SET r = $data ON MATCH SET r = data";		
		
		String assembledQuery = String.format(query, sourceLabel,targetLabel,relationshipLabel);
		System.err.println("Assembled:\n"+assembledQuery);
		Map<String,Object> params = new HashMap<>();
//		params.put("sourceId", m.columnValue(sourceIdProperty));
//		params.put("targetId", m.columnValue(targetIdProperty));
//		params.put("relationshipId", relationshipId);
		params.put("data", parameterList);
		
		
		

		//		System.err.println("Params: "+params);
		
		StatementResult sr = session.run(assembledQuery,params);
		sr.consume();
		long complete = System.currentTimeMillis();

		if(l % 100 == 0) {
			System.err.println("Relationship. Ensure source: "+(complete-started));
		}
	}
	
	
	private void bulkInsertNode(String label, Driver driver, List<ReplicationMessage> m, Function<ReplicationMessage,Optional<String>> nameExtractor) throws Exception {
		if(m.size()==0) {
			return;
		}
		System.err.println("Bulk inserting "+m.size()+" nodes.");
		try(Session session = driver.session()) {
			for (ReplicationMessage replicationMessage : m) {
				insertNode(label, session, replicationMessage, nameExtractor);
			}
		}
		m.get(m.size()-1).commit();
	}	
	private void insertNode(String label, Session session, ReplicationMessage m, Function<ReplicationMessage,Optional<String>> nameExtractor) throws Exception {
		String query = queryNode(m);
		Map<String,Object> v = m.message().flatValueMap("", (key,type,value)->{
			if("string".equals(type)) {
				String val = (String)value;
				return val;
			}
			if("date".equals(type)) {
				Date d = (Date)value;
				return d.getTime();
			}
			return value;
		});
		Optional<String> extractedName = nameExtractor.apply(m);
		String data = "{id: \""+m.queueKey()+"\", "+ (extractedName.isPresent()?"name: \""+extractedName.get()+"\", ":"") + v.keySet().stream().map(s->s+": "+cypherEncode(m.columnValue(s), m.columnType(s))).collect(Collectors.joining(","))+"}";

		final String cypherQuery = "MERGE (a:"+label+" "+query+") set a = "+data+" RETURN a";
//		System.err.println("Data qurey: "+cypherQuery);
		
//	    WITH  {batch_list} AS batch_list
//	    UNWIND batch_list AS rows
//	    WITH rows, toInteger(rows[0]) AS userid
//	    MATCH (u:User {userId: userid})
//	    MERGE (u)-[r:HAS_DAILY_CHARGE]->(n:DailyCharge {userId: toInteger(rows[0])})
//	    ON CREATE SET n.amountUSD = toFloat(rows[1]), n.createdDate = toFloat(rows[2])		
		
		try {
			StatementResult sr = session.run( cypherQuery);
		} catch (Exception e) {
			logger.error("Error: ", e);
			logger.error("Insert failed with query: "+cypherQuery);
			throw e;
		} 

}

	private String queryNode(ReplicationMessage m) {
		String query ="{id:\""+ m.queueKey()+"\"}";
		return query;
	}
	
	private String queryNode2(ReplicationMessage m) {
		String query ="{"+ m.primaryKeys().stream().map(
				key->key+":"+cypherEncode(m.columnValue(key), m.columnType(key))
			).collect(Collectors.joining(","))+"}";
		return query;
	}
}
