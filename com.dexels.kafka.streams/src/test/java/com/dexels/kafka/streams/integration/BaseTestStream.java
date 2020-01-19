package com.dexels.kafka.streams.integration;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.impl.KafkaTopicPublisher;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.api.StreamConfiguration;
import com.dexels.kafka.streams.api.TopologyContext;
import com.dexels.mongodb.sink.MongodbSinkConnector;
import com.dexels.mongodb.sink.MongodbSinkTask;
import com.dexels.pubsub.rx2.api.MessagePublisher;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class BaseTestStream {
    protected StreamConfiguration parseConfig;

    KafkaTopicPublisher publisher;

    private synchronized KafkaTopicPublisher getPublisher() {
        if (publisher != null) {
            return publisher;
        }
        publisher = new KafkaTopicPublisher();
        Map<String, Object> settings = new HashMap<>();
        settings.put("hosts", parseConfig.kafkaHosts());
        settings.put("retries", "20");
        publisher.activate(settings);
        return publisher;

    }

    protected void produceSet1() throws IOException {
    	SecureRandom random = new SecureRandom();
        publishPersonMessage("p1", "wim", 47,0,random);
        publishPersonMessage("p2", "mies", 85,0,random);
        publishPersonMessage("p3", "jet", 23,0,random);
    }
  
    
    protected void produceSet2() throws IOException {
        publishPersonPurchaseMessage("purchase1", "p1", "boot", 1);
        publishPersonPurchaseMessage("purchase2", "p1", "vis", 3);
        publishPersonPurchaseMessage("purchase3", "p1", "brood", 1);
        
        publishPersonPurchaseMessage("purchase4", "p2", "leverworst", 4);
        
        publishPersonPurchaseMessage("purchase5", "p3", "rookkaas", 2);
        publishPersonPurchaseMessage("purchase6", "p3", "smeerworst", 1);
    }

    protected void publishPersonPurchaseMessage(String id, String personid, String item, int quantity) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "101");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("purchaseid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("purchaseid", id);
        columns.put("personid", personid);
        columns.put("item", item);
        columns.put("quantity", quantity);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PURCHASE");
        publisherForTopic.publish(id, m1.toBytes(ReplicationFactory.getInstance()));
        
    }
    
    protected void publishTeamMessage(String teamid, String orgid, String name) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "101");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("teamid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("teamid", teamid);
        columns.put("organizationid", orgid);
        columns.put("name", name);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-TEAM");
        publisherForTopic.publish(teamid, m1.toBytes(ReplicationFactory.getInstance()));
    }
    
    protected void publishLogoMessage(String logoid, String orgid) throws IOException {
        publishLogoMessage(logoid,orgid, null);
    }
    
    protected void publishLogoMessage(String logoid, String orgid, String logocontent) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "101");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("logoid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("logocontent", "een mooie binary");
        if (logocontent != null) {
            columns.put("logocontent", logocontent);

        }
        columns.put("logoid", logoid);
        columns.put("organizationid", orgid);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-LOGO");
        publisherForTopic.publish(logoid, m1.toBytes(ReplicationFactory.getInstance()));
        
    }
    
    protected void publishPersonAttrMessage(String id, String personid, String item, int quantity) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "101");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("attribid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("attribid", id);
        columns.put("personid", personid);
        columns.put("attribname", item);
        columns.put("attribvalue", item);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PURCHASE");
        publisherForTopic.publish(id, m1.toBytes(ReplicationFactory.getInstance()));
        
    }

    protected void publishPersonMessage(String personid, String name, int age,int numberOfGarbageFields, SecureRandom random) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("personid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("personid", personid);
        columns.put("name", name);
        columns.put("age", age);
        IntStream.range(0, numberOfGarbageFields).forEach(i->{
            String key =  "garkey-"+new BigInteger(130, random).toString(32);
            String value = "garvalue-"+ new BigInteger(130, random).toString(32);
            columns.put(key, value);
        });
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PERSON");
        publisherForTopic.publish(personid, m1.toBytes(ReplicationFactory.getInstance()));

    }
    
    
    protected void publishPersonLocationMessage(String personid, String item) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("personid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("personid", personid);
        columns.put("location", item);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PERSONLOCATION");
        publisherForTopic.publish(personid, m1.toBytes(ReplicationFactory.getInstance()));

        
    }
    
    protected void publishPersonLocationMessageSubmsg(String personid, String item) throws IOException {
     
        
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "INSERT");
        dataset.put("PrimaryKeys", Arrays.asList("personid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("personid", personid);
        columns.put("locationitem", item);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        
     
        Map<String,Object> commdata = new HashMap<>();
        Map<String,String> commtypes = new HashMap<>();
        commdata.put("locationname", "aap");
        commtypes.put("locationname", "string");
        ImmutableMessage locationSubMsg = ImmutableFactory.create(commdata,commtypes);
        
        commdata = new HashMap<>();
        commtypes = new HashMap<>();
        commdata.put("addressname", "addressnaam");
        commtypes.put("addressname", "string");
        ImmutableMessage addressSubMsg = ImmutableFactory.create(commdata,commtypes);
        
        locationSubMsg = locationSubMsg.withSubMessage("Address", addressSubMsg);
        
        m1 = m1.withSubMessage("Location", locationSubMsg);

        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PERSONLOCATIONSUB");
        publisherForTopic.publish(personid, m1.toBytes(ReplicationFactory.getInstance()));

        
    }

    
    protected void publishDeletePerson(String personid) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "DELETE");
        dataset.put("PrimaryKeys", Arrays.asList("personid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("personid", personid);
        columns.put("name", null);
        columns.put("age", null);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PERSON");
        publisherForTopic.publish(personid, m1.toBytes(ReplicationFactory.getInstance()));

    }
    
    
    protected void publishDeleteLogo(String logoid) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "DELETE");
        dataset.put("PrimaryKeys", Arrays.asList("logoid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("logoid", logoid);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-LOGO");
        publisherForTopic.publish(logoid, m1.toBytes(ReplicationFactory.getInstance()));

    }
    
    
    protected void publishDeleteTeam(String teamid) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "DELETE");
        dataset.put("PrimaryKeys", Arrays.asList("teamid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("teamid", teamid);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-TEAM");
        publisherForTopic.publish(teamid, m1.toBytes(ReplicationFactory.getInstance()));

    }
    
    protected void publishDeletePersonPurchase(String purchaseId) throws IOException {
        Map<String, Object> dataset = new HashMap<>();
        dataset.put("TransactionId", "100");
        dataset.put("Timestamp", new Date().getTime());
        dataset.put("Operation", "DELETE");
        dataset.put("PrimaryKeys", Arrays.asList("purchaseid"));
        Map<String, Object> columns = new HashMap<>();
        dataset.put("Columns", columns);
        columns.put("purchaseid", purchaseId);
        columns.put("personid", null);
        columns.put("item", null);
        columns.put("quantity", null);
        ReplicationMessage m1 = ReplicationFactory.create(dataset);
        MessagePublisher publisherForTopic = getPublisher().publisherForTopic("testcase-develop-TestStreamJoin-PURCHASE");
        publisherForTopic.publish(purchaseId, m1.toBytes(ReplicationFactory.getInstance()));

    }



    protected void reset(String generation) {
        deleteTopic("testcase-develop-TestStreamJoin-PURCHASE");
        deleteTopic("testcase-develop-TestStreamJoin-PERSON");
        deleteTopic("testcase-develop-TestStreamJoin-PERSONLOCATION");
        deleteTopic("testcase-develop-TestStreamJoin-TEAM");
        deleteTopic("testcase-develop-TestStreamJoin-LOGO");
    }
    protected void resetAll(String generation) {
        Map<String,Object> config = new HashMap<>();
        config.put("bootstrap.servers", parseConfig.kafkaHosts());
        Set<String> topics;
        
        try(AdminClient ac = AdminClient.create(config)) {
      
            topics = ac.listTopics().names().get();
            for (String topic : topics) {
                if (topic.contains("TestPerformance")) {
                    ac.deleteTopics(Collections.singletonList(topic));
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void deleteTopic(String topic) {
        Map<String,Object> config = new HashMap<>();
        config.put("bootstrap.servers", parseConfig.kafkaHosts());
        try(AdminClient ac = AdminClient.create(config)) {
            ac.deleteTopics(Collections.singletonList(topic));

        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
    
    

    protected void dropMongo(String generation) {
        getMongoDatabase(generation).drop();
    }
    
    protected MongoDatabase getMongoDatabase(String generation) {
        StreamConfiguration streamConfiguration = parseConfig;
        Map<String, String> map = streamConfiguration.sinks()
                .get("replication")
                .settings();

        TopologyContext context = new TopologyContext(Optional.of("testcase"), "develop", map.get("database"), "generation"+generation);
        String resolvedDatabase = CoreOperators.generationalGroup(this.getClass().getSimpleName(),context);

         MongoClient mongoClient = MongodbSinkTask.createClient(map.get(MongodbSinkConnector.HOST),
                map.get(MongodbSinkConnector.PORT));
         return mongoClient.getDatabase(resolvedDatabase);

    }
   

}
