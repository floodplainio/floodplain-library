package io.floodplain.debezium;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;

public class SimpleDebezium {

	public static void main(String[] args) {
		Configuration config = configurePostgres("dvdaa","postgres",5432,"postgres","mysecretpassword","dvdrental");
		EmbeddedEngine engine = EmbeddedEngine.create()
		        .using(config)
		        .notifying(SimpleDebezium::consumeRecord)
		        .build();
		
		Executor executor = Executors.newSingleThreadExecutor();
		executor.execute(engine);
	}
	
	public static void consumeRecord(SourceRecord sr) {
		System.err.println("Source: "+sr.topic()+" off: "+sr.sourceOffset()+" size: "+sr.value());
	}

	private static Configuration configurePostgres(String name, String hostname, int port, String user, String password, String database) {
		return Configuration.create()
		        .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
		        .with("name", name)
		        .with("database.server.name", name)
		        .with("database.hostname", hostname)
		        .with("database.port", port)
		        .with("database.user", user)
		        .with("database.password", password)
		        .with("database.dbname", database)
		        .with("offset.storage.topic", "storage-"+name)
		        .with("offset.storage.partitions", 5)
		        .with("offset.storage.replication.factor", 1)
		        .build();
		
	}
	
//	  "config": {
//		    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
//		    "database.hostname": "postgres",
//		    "database.port": "5432",
//		    "database.user": "postgres",
//		    "database.password": "mysecretpassword",
//		    "database.dbname" : "dvdrental",
//		    "database.server.name": "dvd"
//		  }

}
