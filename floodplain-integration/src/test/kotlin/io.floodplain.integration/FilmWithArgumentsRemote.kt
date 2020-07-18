package io.floodplain.integration

import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.floodplain.mongodb.waitForMongoDbCondition
import io.floodplain.test.useIntegraton
import kotlin.test.assertNotNull
import org.junit.Ignore
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

public class FilmWithArgumentsRemote {
    /**
     * Re-enable when I've got a testcontainer based kafka cluster
     */
    @Test @Ignore
    fun testPostgresRemoteSourceFromArguments() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        stream {

            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                "postgres",
                5432,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://mongo:27017",
                "@mongodump"
            )
            postgresSource("film", postgresConfig) {
                // Clear the last_update field, it makes no sense in a denormalized situation
                set { _, film, _ ->
                    film["last_update"] = null; film
                }
                mongoSink("filmwithactors", "@filmwithcat", mongoConfig)
            }
        }.runWithArguments("--kafka", "localhost:9092", "--connect", "http://localhost:8083/connectors") { topologyContext ->
            val database = topologyContext.topicName("@mongodump")
            // flushSinks()
            val hits = waitForMongoDbCondition("mongodb://localhost:27017", database) { currentDatabase ->
                val collection = currentDatabase.getCollection("filmwithactors")
                val countDocuments = collection.countDocuments()
                if (countDocuments == 1000L) {
                    1000L
                } else {
                    null
                }
            }
            assertNotNull(hits)
        }
    }
}
