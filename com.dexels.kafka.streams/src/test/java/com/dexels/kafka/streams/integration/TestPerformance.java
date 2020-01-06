package com.dexels.kafka.streams.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.Topology;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.base.StreamRuntime;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import com.mongodb.client.MongoCollection;

public class TestPerformance extends BaseTestStream {
    public static String GENERATION = "3";
    private StreamInstance instance;

    
    @Before
    public void setup() throws IOException, InterruptedException {
        System.out.println("Starting new run");
//        ReplicationFactory.setInstance(new ReplicationMessageParserImpl());
        ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

        Reader r = new InputStreamReader(TestPerformance.class.getResourceAsStream("resources.xml"));
		parseConfig = StreamRuntime.parseConfig("develop", r,null);
        this.dropMongo(GENERATION);
        System.out.println("Resetting streams application");
        this.reset(GENERATION);
        this.resetAll(GENERATION);
        Thread.sleep(5000);
        instance = new StreamInstance(this.getClass() .getSimpleName(), parseConfig, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    }

    @After
    public void clear() throws InterruptedException {
        try {
            instance.shutdown();

        } catch (Throwable t) {
            t.printStackTrace();
        }

        try {
            System.out.println("Resetting kafka streams");
            this.reset(GENERATION);
            parseConfig = null;
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
        
    }
    
    @Test @Ignore
    public void testPerformanceMultidirect() throws IOException, InterruptedException, ExecutionException {
    	Topology topology = new Topology();
    	File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin1.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
                
                for (int j=0;j<4;j++) {
                    String item =  new BigInteger(130, random).toString(5);
                    publishPersonPurchaseMessage(personid+j, personid, item,2  );
                }
            }
            System.out.println("Finished producing");
            Thread.sleep(1000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Purchase.3\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            while (mongocount < 10000) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            }
            
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            
        }
    }
    
    @Test @Ignore
    public void testPerformanceLowlevelManyToOne() throws IOException, InterruptedException, ExecutionException {
    	Topology topology = new Topology();
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelManyToOne.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  "person" + new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,2,random);
                
                for (int j=0;j<4;j++) {
                    String item = "purchase" +  new BigInteger(130, random).toString(5);
                    publishPersonPurchaseMessage("purchase" + personid+j, personid, item,2  );
                }
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Person\": {$exists: true}}");
            MongoCollection<Document> collection = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1");
            long mongocount = collection.countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (20*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = collection.countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertTrue(mongocount >= 10000);
           
            
  
            System.out.println("Insert a purchase without person");
            String item = "purchase" +  new BigInteger(130, random).toString(5);
            publishPersonPurchaseMessage("purchasetest", "mies", item, 2 );
            
            Thread.sleep(4000);
            query =  Document.parse("{\"_id\": \"purchasetest\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount);
           
            System.out.println("Insert the person for the above purchase");
            publishPersonMessage("mies", "Jannie", 50,2,random);
            Thread.sleep(5000);
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            System.out.println("Update the purchase (forward update)");
            publishPersonPurchaseMessage("purchasetest", "mies", "Suikerbieten", 2 );
            Thread.sleep(5000);
            query =  Document.parse("{\"_id\": \"purchasetest\", \"item\": \"Suikerbieten\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            System.out.println("Update the person (reverse update)");
            publishPersonMessage("mies", "Jannie52", 50,2,random);
            Thread.sleep(5000);
            query =  Document.parse("{\"_id\": \"purchasetest\", \"Person.name\": \"Jannie52\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            System.out.println("Delete the person  (reverse delete)");
            publishDeletePerson("mies");
            Thread.sleep(5000);
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount);
            
            System.out.println("Insert person again, and delete it the purchase (forward delete)");
            publishPersonMessage("mies", "Jannie52", 50,2,random);
            Thread.sleep(5000);
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            publishDeletePersonPurchase("purchasetest");
            Thread.sleep(5000);
            query =  Document.parse("{\"_id\": \"purchasetest\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount);
            
        }
    }
    
    @Test
    @Ignore
    public void testPerformanceLowlevelOneToOne() throws IOException, InterruptedException, ExecutionException {
    	Topology topology = new Topology();
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelOneToOne.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,100,random);
                String location =  new BigInteger(130, random).toString(5);
                publishPersonLocationMessage(personid,  location);
            
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology,resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Location\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (2*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertEquals(10000, mongocount );
        }
    }
    
    @Ignore
    @Test
    public void testPerformanceLowlevelOneToOneColumns() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
    	Topology topology = new Topology();
        SecureRandom random = new SecureRandom();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelOneToOneColumns.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
                String location =  new BigInteger(130, random).toString(5);
                publishPersonLocationMessageSubmsg(personid,  location);
            
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"addressname\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (2*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertEquals(10000, mongocount );
        }
    }
    
    
  
    @Test   @Ignore
    public void testPerformanceLowlevelManyToMany() throws IOException, InterruptedException, ExecutionException {

        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelManyToMany.xml")) {
            for (int i=0;i<10000;i++) {
                String teamid =  "team" + new BigInteger(130, random).toString(32);
                String orgid =  "org" + new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishTeamMessage(teamid, orgid, name);
                
                for (int j=0;j<4;j++) {
                    String logoid =  "logo" + new BigInteger(130, random).toString(32);
                    publishLogoMessage(logoid, orgid);
                }
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
//            
            Document query =  Document.parse("{\"Logo.3\": {$exists: true}}");
            MongoCollection<Document> collection = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1");
            long mongocount = collection.countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (20*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = collection.countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertTrue(mongocount >= 10000);

            
            System.out.println("Insert a logo without matching team");
            String orgid = "testorg";
            String logoid = "testlogo";
            publishLogoMessage(logoid, orgid);
            Thread.sleep(4000);
            query =  Document.parse("{\"orgid\": \"testorg\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount);
            
            System.out.println("Insert Team without Logo");
            String team1id = "team1";
            String orgid2 = "testorg2";
            publishTeamMessage(team1id, orgid2, "TestTeam1");
            Thread.sleep(4000);
            query =  Document.parse("{\"_id\": \"team1\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            query =  Document.parse("{\"_id\": \"team1\", \"Logo\": {\"$exists\": 0}}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            
            System.out.println("Insert Team with Logo");
            String team2id = "team2";
            publishTeamMessage(team2id, orgid, "TestTeam2");
            Thread.sleep(4000);
            query =  Document.parse("{\"_id\": \"team2\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            query =  Document.parse("{\"_id\": \"team2\", \"Logo.0\": {\"$exists\": 1}}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            System.out.println("Update a logo (reverse update");
            
            publishLogoMessage(logoid, orgid, "ABC");
            Thread.sleep(10000);
            query =  Document.parse("{\"_id\": \"team2\", \"Logo.0.logocontent\": \"ABC\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            System.out.println("Update a Team (forward update)");
            publishTeamMessage(team2id, orgid, "TestTeamMIES");
            Thread.sleep(10000);
            query =  Document.parse("{\"_id\": \"team2\", \"name\": \"TestTeamMIES\", \"Logo.0.logocontent\": \"ABC\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount);
            
            
            System.out.println("Delete a logo (reverse delete");
            publishDeleteLogo(logoid);
            Thread.sleep(10000);
            query =  Document.parse("{\"_id\": \"team2\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(1, mongocount); // we should still have the team
            
            query =  Document.parse("{\"_id\": \"team2\", \"Logo.0\": {\"$exists\": 1}}");
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount); // but not the logo
            
            
            System.out.println("Delete a team (forward delete");
            publishDeleteTeam(team2id);
            Thread.sleep(10000);
            query =  Document.parse("{\"_id\": \"team2\"}");
            mongocount = collection.countDocuments(query);
            assertEquals(0, mongocount);

        }
    
    }
    
    
    
    @Test  @Ignore
    public void testPerformanceLowlevelOneToMany() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelOneToMany.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  "person" + new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
                for (int j=0;j<4;j++) {
                    String item =  new BigInteger(130, random).toString(5);
                    publishPersonPurchaseMessage("purs" + personid+j, personid, item,2  );
                }
            
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Purchase.3\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (10*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertTrue(mongocount >= 10000);
            
            // Insert two persons to do delete tests with
            String personid =  "Jan";
            String name =  new BigInteger(130, random).toString(32);
            publishPersonMessage(personid, name, 50,0,random);
            for (int j=0;j<4;j++) {
                String item =  new BigInteger(130, random).toString(5);
                publishPersonPurchaseMessage(personid+j, personid, item,2  );
            }
            personid =  "Pieter";
            name =  new BigInteger(130, random).toString(32);
            publishPersonMessage(personid, name, 50,0,random);
            for (int j=0;j<4;j++) {
                String item =  new BigInteger(130, random).toString(5);
                publishPersonPurchaseMessage(personid+j, personid, item,2  );
            }
            // Ensure they exist
            query =  Document.parse("{_id: \"Jan\", \"Purchase.3\": {$exists: true}}");
            mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            runtime = 0;
            while (mongocount < 1 && runtime < (2*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            
            assertEquals(1, mongocount);
            query =  Document.parse("{_id: \"Pieter\", \"Purchase.3\": {$exists: true}}");
            mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            assertEquals(1, mongocount);
            System.out.println("Inserted two more persons. Going to delete one");
            // Delete a person and ensure its gone
            publishDeletePerson("Jan");
            Thread.sleep(10000);
            query =  Document.parse("{_id: \"Jan\"}");
            mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            assertEquals(0, mongocount);
            System.out.println("One deleted successfully. Going to update its purchase");
            
            // Update one of the deleted person purchases and ensure its not back
            publishPersonPurchaseMessage("Jan"+1, "Jan", "aap", 1  );
            Thread.sleep(10000);
            query =  Document.parse("{_id: \"Jan\"}");
            mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            assertEquals(0, mongocount);
            System.out.println("Person still deleted after updating its purchase");
            
            // Delete a purchase of a difference person and ensure its removed
            publishDeletePersonPurchase("Pieter1");
            Thread.sleep(10000);
            query =  Document.parse("{_id: \"Pieter\"}");
            List<Document> result = new ArrayList<>();
            getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").find(query).into(result);
            assertEquals(1, result.size());
            System.out.println("Deleted a purchase but person still there");
            Document pieter = result.get(0);
            @SuppressWarnings("unchecked")
            List<Document> purlist = (List<Document>) pieter.get("Purchase");
            assertNotNull(purlist);
            assertEquals(3, purlist.size());
            
        }
    }
    
    
    @Test
    @Ignore
    public void testPerformanceLowlevelOneToManyKeyValue() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin_lowlevelOneToManyKeyValue.xml")) {
            for (int i=0;i<10000;i++) {
                String personid =  "person" + new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
                for (int j=0;j<4;j++) {
                    String item =  new BigInteger(130, random).toString(5);
                    publishPersonPurchaseMessage("purs" + personid+j, personid, item,2  );
                }
            
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Purchase.3\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            long now = System.currentTimeMillis();
            long runtime = 0;
            while (mongocount < 10000 && runtime < (10*60*1000L)) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
                runtime = System.currentTimeMillis() - now;
            }
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");
            assertTrue(mongocount >= 10000);
            
        
        }
    }
    

    @Test
    @Ignore
    public void testPerformanceSingleMerge() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin4.xml")) {
            for (int i=0;i<100000;i++) {
                String personid =  new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
                
                String location =  new BigInteger(130, random).toString(5);
                publishPersonLocationMessage(personid, location);
                
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            Document query =  Document.parse("{\"Location\": {$exists: true}}");
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            while (mongocount < 100000) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments(query);
            }
            
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");

            
        }
    }

    @Test
    @Ignore
    public void testPerformanceSimpleJoin() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
        SecureRandom random = new SecureRandom();
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin3.xml")) {
            for (int i=0;i<500000;i++) {
                String personid =  new BigInteger(130, random).toString(32);
                String name =  new BigInteger(130, random).toString(32);
                publishPersonMessage(personid, name, 50,0,random);
            }
            System.out.println("Finished producing");
            Thread.sleep(10000);
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            Long started = new Date().getTime();
            System.out.println("Started testjoin1 instance");
            
            long mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments();
            while (mongocount < 500000) {
                System.out.println("Going to sleep to let kafka run. " + mongocount);
                Thread.sleep(2000);
                mongocount = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").countDocuments();
            }
            
            Long stopped = new Date().getTime();
            System.out.println("Finished in " + (stopped-started) +  "ms!");

            
            
        }
    }
}
