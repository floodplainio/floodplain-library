package com.dexels.kafka.streams.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
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
import com.mongodb.client.FindIterable;

public class TestStreamJoins extends BaseTestStream {
    public static String GENERATION = "1";
    private StreamInstance instance;

    @Before
    public void setup() throws IOException, InterruptedException {
        System.out.println("Starting new run");

        Reader r = new InputStreamReader(TestStreamJoins.class.getResourceAsStream("resources.xml"));
        parseConfig = StreamRuntime.parseConfig("develop", r);
        this.dropMongo(GENERATION);
        System.out.println("Resetting streams application");
        this.reset(GENERATION);
        Thread.sleep(5000);
        instance = new StreamInstance(this.getClass() .getSimpleName(), parseConfig, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    }

    @After
    public void clear() {
        try {
            parseConfig = null;
            System.out.println("Resetting kafka streams");
            this.reset(GENERATION);
           
        } catch (Throwable t) {
            t.printStackTrace();
        }
        
        
    }

    @Test
    @Ignore
    public void testJoin1() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
    	Topology topology = new Topology();
        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin1.xml")) {
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            System.out.println("Started testjoin1 instance");
            
            this.produceSet1();
            this.produceSet2();
            System.out.println("Produced sets - going to sleep to let kafka run");

            Thread.sleep(30000);
            
            System.out.println("Stopping kafka instance");
            try {
                instance.shutdown();

            } catch (Throwable t) {
                t.printStackTrace();
            }

            System.out.println("Starting asserts");
            assertEquals(3, getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").count());
            Document q1 = new Document();
            q1.put("_id", "p1");
            FindIterable<Document> find = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").find(q1);
            List<Document> result = find.into(new ArrayList<>());
            assertEquals(1, result.size());
            Document p1 = result.get(0);
            @SuppressWarnings("unchecked")
            List<Document> purchase = (List<Document>) p1.get("Purchase");
            assertNotNull(purchase);
            assertEquals(3, purchase.size());
            
        }
    }
    
    @Test
    @Ignore
    public void testDelete1() throws IOException, InterruptedException, ExecutionException {

        File output = new File("../cnf/storage_kafka");
    	Topology topology = new Topology();

        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin1.xml")) {
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            System.out.println("Started testDelete1 instance");

            
            this.produceSet1();
            this.produceSet2();
            System.out.println("Produced sets - going to sleep to let kafka run");


            Thread.sleep(30000);
            System.out.println("Deleting p1");

            publishDeletePerson("p1");
            Thread.sleep(5000);
            System.out.println("Stopping kafka instance");
            try {
                instance.shutdown();

            } catch (Throwable t) {
                t.printStackTrace();
            }
            
            System.out.println("Starting asserts");

            assertEquals(2, getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").count());
            Document q1 = new Document();
            q1.put("_id", "p1");
            FindIterable<Document> find = getMongoDatabase(GENERATION).getCollection("TestStreamJoin1").find(q1);
            List<Document> result = find.into(new ArrayList<>());
            assertEquals(0, result.size());
            
        }
    }
    
    @Test
    @Ignore
    public void testDelete2() throws IOException, InterruptedException, ExecutionException {
        File output = new File("../cnf/storage_kafka");
    	Topology topology = new Topology();
        try (InputStream resourceAsStream = TestStreamJoins.class.getResourceAsStream("TestStreamJoin2.xml")) {
            instance.parseStreamMap(topology, resourceAsStream, output, "develop", GENERATION,Optional.empty());
            instance.start();
            System.out.println("Started testDelete2 instance");

            
            this.produceSet1();
           
            System.out.println("Produced sets - going to sleep to let kafka run");

            Thread.sleep(30000);
            System.out.println("Deleting p1");

            publishDeletePerson("p1");
            Thread.sleep(5000);
            System.out.println("Stopping kafka instance");
            try {
                instance.shutdown();

            } catch (Throwable t) {
                t.printStackTrace();
            }
            
            System.out.println("Starting asserts");

            assertEquals(2, getMongoDatabase(GENERATION).getCollection("TestStreamJoin2").count());
            Document q1 = new Document();
            q1.put("_id", "p1");
            FindIterable<Document> find = getMongoDatabase(GENERATION).getCollection("TestStreamJoin2").find(q1);
            List<Document> result = find.into(new ArrayList<>());
            assertEquals(0, result.size());
            
        }
    }
}
