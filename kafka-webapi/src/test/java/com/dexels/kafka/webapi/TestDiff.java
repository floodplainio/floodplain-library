package com.dexels.kafka.webapi;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class TestDiff {

    private static final List<String> IGNORE = Arrays.asList(new String[]{"replicated", "updateby", "lastupdate", "relationend", "info"});

    @Test
    @Ignore
    public void performTest() {
        MongoClient mongoClient = new MongoClient("mongo-test-0.mongo-test.test.svc.cluster.local", 27017);
        diffGenerations(mongoClient, "KNBSB", "test", "20191026", "20191118e", Optional.empty());
//		diffDatabasesForCollection("TeamPerson", old, newD);
    }

    @Test
    @Ignore
    public void performDiffSingle() {
        MongoClient mongoClient = new MongoClient("mongo-test-0.mongo-test.test.svc.cluster.local", 27017);
        diffSingle(mongoClient, "KNBSB", "test", "20191026", "20191119", "Facility");
    }

    private void diffSingle(MongoClient mongoClient, String tenant, String deployment, String oldGen, String newGen, String collection) {
        MongoDatabase old = generationalDatabase(mongoClient, tenant, deployment, oldGen); // mongoClient.getDatabase("TENANT-test-generation-160119-total-replication");
        MongoDatabase newD = generationalDatabase(mongoClient, tenant, deployment, newGen); // .getDatabase("TENANT-test-generation-20190820-total-replication");
        diffDatabasesForCollection(collection, old, newD);
    }

    private void diffGenerations(MongoClient mongoClient, String tenant, String deployment, String oldGen, String newGen, Optional<List<String>> onlyCollections) {
        MongoDatabase old = generationalDatabase(mongoClient, tenant, deployment, oldGen); // mongoClient.getDatabase("TENANT-test-generation-160119-total-replication");
        MongoDatabase newD = generationalDatabase(mongoClient, tenant, deployment, newGen); // .getDatabase("TENANT-test-generation-20190820-total-replication");
        globalDiff(old, newD, onlyCollections);
    }

    private MongoDatabase generationalDatabase(MongoClient mongoClient, String tenant, String deployment, String generation) {
        String dbName = tenant + "-" + deployment + "-generation-" + generation + "-total-replication";
        System.err.println("Getting database: " + dbName);
        return mongoClient.getDatabase(dbName);
    }

    @Test
    @Ignore
    public void testSingle() throws IOException {

        Document first = parseDocumentFromClassPath("diff1.json");
        Document second = parseDocumentFromClassPath("diff2.json");
        Assert.assertTrue(symEquals(first, second, IGNORE));
    }

    private static Predicate<String> includeCollection(Optional<List<String>> onlyCollections) {
        if (onlyCollections.isPresent()) {
            return coll -> onlyCollections.get().contains(coll);
        } else {
            return e -> true;
        }
    }

    private static void globalDiff(MongoDatabase old, MongoDatabase newD, Optional<List<String>> onlyCollections) {
        List<String> oldNames = StreamSupport.stream(old.listCollectionNames().spliterator(), false)
                .filter(includeCollection(onlyCollections))
                .collect(Collectors.toList());
        List<String> newNames = StreamSupport.stream(newD.listCollectionNames().spliterator(), false)
                .filter(includeCollection(onlyCollections))
                .collect(Collectors.toList());

//		if(oldNames.equals(newNames)) {
//			System.err.println("Structural equal: "+oldNames);
//			oldNames.forEach(e->{
//				diffDatabasesForCollection(e, old, newD);
//			});
//		} else {

        final List<String> diffList = diffList(oldNames, newNames);
        System.err.println("Structural not equal. Diff: " + diffList);
        List<String> filteredDiff = diffList.stream().filter(e -> !e.contains("Debug")).collect(Collectors.toList());
        if (filteredDiff.isEmpty()) {
            System.err.println("But empty after filtering. Continuing.");
            oldNames.forEach(e -> {
                diffDatabasesForCollection(e, old, newD);
                estimateDiffCollections(old, newD, e);
            });
        }
//		}

    }

    private static List<String> diffList(List<String> aList, List<String> bList) {
        return aList.stream().filter(aObject -> !bList.contains(aObject)).collect(Collectors.toList());
    }

    private static void diffDatabasesForCollection(String collection, MongoDatabase old, MongoDatabase newD) {
        System.err.println("Checking collection: " + collection);
        MongoCollection<Document> oldC = old.getCollection(collection);
        MongoCollection<Document> newC = newD.getCollection(collection);
        diffCollections(oldC, newC, IGNORE, Optional.of(200));
//		diffCollections(newC,oldC,IGNORE);
    }

    //	Document stats = database.runCommand(new Document("collStats", "myCollection"));
    private static boolean collectionExists(MongoDatabase db, final String collectionName) {
        List<String> collectionNames = StreamSupport.stream(db.listCollectionNames().spliterator(), false).collect(Collectors.toList());
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    private static void estimateDiffCollections(MongoDatabase old, MongoDatabase newD, String collectionName) {
        if (!collectionExists(old, collectionName) || !collectionExists(newD, collectionName)) {
            System.err.println("Collection not found in both. Ignoring for the rest");
            return;
        }

        final Document oldStatsFull = old.runCommand(new Document("collStats", collectionName));
//		System.err.println("OLDSTAT: "+oldStatsFull);
        final Document newStatsFull = newD.runCommand(new Document("collStats", collectionName));
        long oldDocumentCount = oldStatsFull.getInteger("count");
        long newDocumentCount = newStatsFull.getInteger("count");
        if (oldDocumentCount != newDocumentCount) {
            System.err.println("Document count difference. OldCount: " + oldDocumentCount + " newcount: " + newDocumentCount);
            return;
        }
        int oldStats = oldStatsFull.getInteger("storageSize");
        int newStats = newStatsFull.getInteger("storageSize");
        double relativeDiff = (double) oldStats / (double) newStats;
        if (relativeDiff < 0.85 || relativeDiff > 1.15) {
            System.err.println("Significant difference found: " + relativeDiff + " for collection: " + collectionName);
            System.err.println("OLD EST: " + oldStats);
            System.err.println("NEW EST: " + newStats);
//			diffCollections(old.getCollection(collectionName), newD.getCollection(collectionName), IGNORE);
        }
    }

    private static void diffCollections2(MongoCollection<Document> oldC, MongoCollection<Document> newC, List<String> ignoredFields) {
        //		StreamSupport.stream(
//		StreamSupport.stream(oldC.find().spliterator(),false).collect(Collectors.partitioningBy(predicate))
    }

    private static void diffCollections(MongoCollection<Document> oldC, MongoCollection<Document> newC, List<String> ignoredFields, Optional<Integer> limit) {


        long oldCount = oldC.countDocuments();
        long newCount = newC.countDocuments();
        System.err.println("Old: " + oldCount + " New: " + newCount);

//		Document query = new Document("childorganizationid","D1C8S7R");
        AtomicLong counter = new AtomicLong(0);
        StreamSupport.stream(oldC.find().spliterator(), false)
                .limit(limit.orElse(Integer.MAX_VALUE))
                .forEach(new Consumer<Document>() {

                    @Override
                    public void accept(Document doc) {
                        String id = doc.getString("_id");
                        Document newDoc = find(newC, id);
                        long c = counter.incrementAndGet();
//				if(c % 1000 == 0 ) {
                        System.err.println("Progress: " + c + "/" + oldCount);
//				}
                        if (newDoc == null) {
                            System.err.println("No matching doc for id: " + id);
                        } else {
                            boolean isEquals = symEquals(doc, newDoc, ignoredFields);
                            if (!isEquals) {
                                System.err.println(">\n" + doc.toJson());
                                System.err.println("<\n" + newDoc.toJson());
                            }
                        }

                    }
                });
    }

//	private static Predicate<Document> 


    private static boolean equalsDoc(Document doc, Document newDoc, List<String> ignoredFields) {
        for (Entry<String, Object> e : doc.entrySet()) {
            if (ignoredFields.contains(e.getKey())) {
                continue;
            }
//			System.err.println("Field: "+e.getKey()+" is not in "+ignoredFields);
            Object other = newDoc.get(e.getKey());
            boolean diff = objectEquals(e.getValue(), other, ignoredFields);
            if (!diff) {
                System.err.println("Not equal! Key: " + e.getKey() + " type: " + e.getValue().getClass() + " this: " + e.getValue() + " other: " + other);
                return false;
            }
        }
        return true;
    }

    private static boolean symEquals(Document doc, Document newDoc, List<String> ignoredFields) {
        return equalsDoc(doc, newDoc, ignoredFields) && equalsDoc(newDoc, doc, ignoredFields);
    }


    private static boolean objectEquals(Object value, Object other, List<String> ignoredFields) {
        if (other == null) {
            return false;
        }
        if (value == null) {
            return false;
        }
        if (value instanceof Document && other instanceof Document) {
            return symEquals((Document) value, (Document) other, ignoredFields);
        }
        if (value instanceof List && other instanceof List) {
            List<Object> o = (List<Object>) value;
            List<Object> p = (List<Object>) other;
            if (true) {
                return true;
            }
            if (o.size() != p.size()) {
                System.err.println("different lengths: " + o.size() + " vs. " + p.size());
                return false;
            }
            int i = 0;
            for (Object object : p) {
                if (!objectEquals(object, p.get(i++), ignoredFields)) {
                    return false;
                }
            }
            return true;
        }
        return value.equals(other);
    }

    private static Document find(MongoCollection<Document> collection, String id) {
        return collection.find(Document.parse("{'_id' :'" + id + "'}")).first();
    }

    public final Document parseDocumentFromClassPath(String path) throws IOException {
        InputStream inputStream = getClass().getResourceAsStream(path);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        byte[] byteArray = buffer.toByteArray();

        String text = new String(byteArray, StandardCharsets.UTF_8);
        return Document.parse(text);
    }
}
