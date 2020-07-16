import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleMySQL {
    public static void main(String[] args) throws IOException, InterruptedException {


// Define the configuration for the Debezium Engine with MySQL connector...
        final Properties props = new Properties();
        props.setProperty("name", "engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "offsets.dat");
        props.setProperty("offset.flush.interval.ms", "5000");
        /* begin connector properties */

        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.server.name", "instance-mysqlsource");
        props.setProperty("database.dbname", "my-wpdb");
        props.setProperty("database.user", "root");
        props.setProperty("database.password", "mysecretpassword");

        props.setProperty("database.server.id", "85744");
        props.setProperty("database.history",
                "io.debezium.relational.history.FileDatabaseHistory");
        props.setProperty("database.history.file.filename",
                "dbhistory.dat");

// Create the engine with this configuration ...
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    System.out.println(record);
                }).build()
        ) {
            Executors.newSingleThreadExecutor().execute(engine);

            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Do something else or wait for a signal or an event
            Thread.sleep(100000);
        }
// Engine is stopped when the main code is finished


    }
}
