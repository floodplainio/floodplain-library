package com.dexels.kafka.streams.testdata;

import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class TestIndices {

	private final static Logger logger = LoggerFactory.getLogger(TestIndices.class);

	
	@Test @Ignore
	public void testIndex() {
		MongoClient client = new MongoClient("10.0.0.8");
		MongoDatabase db = client.getDatabase("KNBSB-develop-generation-133-total-replication");
//		MongoCollection<Document> coll = db.getCollection("ClassSeason");
		MongoCollection<Document> coll = db.getCollection("Season");
		for (Document d : coll.listIndexes()) {
			System.err.println(d);
		}
//		coll.listIndexes().forEach((Document d)->System.err.println(d));
		createIndex(coll,"classid:1,competitiontypeid:1,organizingdistrictid:1",false);
		client.close();
	}

	private void createIndex(MongoCollection<Document> coll, String definition, boolean unique) {
		List<String> part = Arrays.asList(definition.split(","));
		Document d = new Document();
		for (String pt : part) {
			List<String> prts = Arrays.asList(pt.split(":"));
			String name = prts.get(0);
			int direction = Integer.parseInt(prts.get(1));
			
			d.put(name, direction);
		}
//		Document d = new Document("TeamId",1);
//		Document options = new Document();
		System.err.println("> "+d);
		IndexOptions indexOptions = new IndexOptions();
		if(unique) {
			indexOptions.unique(true);
//			options.put("unique", true);
		}
		logger.info("Created index on collection: {} with definition: {}",coll.getNamespace().getCollectionName(),definition);
		String result = coll.createIndex(d,indexOptions);
		logger.info("Created index result: {}",result);
	}

//	private void createIndex(MongoCollection<Document> coll, String definition, boolean unique) {
//		List<String> part = Arrays.asList(definition.split(","));
//		Document d = new Document();
//		for (String pt : part) {
//			List<String> prts = Arrays.asList(pt.split(":"));
//			String name = prts.get(0);
//			int direction = Integer.parseInt(prts.get(1));
//			
//			d.put(name, direction);
//		}
//		IndexOptions indexOptions = new IndexOptions();
//		if(unique) {
//			indexOptions.unique(true);
//		}
//		coll.createIndex(d,indexOptions);
//	}
}
