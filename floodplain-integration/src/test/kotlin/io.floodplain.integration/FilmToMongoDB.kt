package io.floodplain.integration

import com.mongodb.client.MongoClients
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import kotlin.test.assertEquals
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class FilmToMongoDB {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @After
    fun shutdown() {
        postgresContainer.close()
        mongoContainer.close()
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
    @Test
    fun testPostgresSource() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        stream {

            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}",
                "@mongodump"
            )
            postgresConfig.sourceSimple("film") {
                // Clear the last_update field, it makes no sense in a denormalized situation
                set { _, film, _ ->
                    film["last_update"] = null; film
                }
                // Join with something that also uses the film_id key space.
                // optional = true so films without any actors (animation?) will propagate
                // multiple = true we're not joining with something that actually 'has' a film id
                // we are joining with something that is grouped by film_id
                joinGrouped(optional = true) {

                    postgresConfig.sourceSimple("film_actor") {
                        joinRemote({ msg -> "${msg["actor_id"]}" }, false) {
                            postgresConfig.sourceSimple("actor") {
                            }
                        }
                        // copy the first_name, last_name and actor_id to the film_actor message, drop the last update
                        set { _, actor_film, actor ->
                            actor_film["last_name"] = actor["last_name"]
                            actor_film["first_name"] = actor["first_name"]
                            actor_film["actor_id"] = actor["actor_id"]
                            actor_film["last_update"] = null
                            actor_film
                        }
                        // group the film_actor stream by film_id
                        group { msg -> "${msg["film_id"]}" }
                    }
                }
                // ugly hack: As lists of messages can't be toplevel, a grouped message always consist of a single, otherwise empty message, that only
                // contains one field, which is a list of the grouped messages, and that field is always named 'list'
                // Ideas welcome
                set { _, film, actorlist ->
                    film["actors"] = actorlist["list"] ?: emptyList<IMessage>()
                    film
                }
                // pass this message to the mongo sink
                mongoSink("filmwithactors", "@filmwithcat", mongoConfig)
                // mongoSink("film", "@film", mongoConfig)
            }
        }.renderAndTest {
            logger.info("Outputs: ${outputs()}")
            delay(30000)
            val database = topologyContext().topicName("@mongodump")
            connectJobs().forEach { it.cancel("ciao!") }
            var hits = 0L
            withTimeout(200000) {
                repeat(1000) {
                    MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}")
                        .use { client ->
                            val collection = client.getDatabase(database).getCollection("filmwithactors")
                            hits = collection.countDocuments()
                            logger.info("Count of Documents: $hits in database: $database")
                            if (hits == 1000L) {
                                return@withTimeout
                            }
                        }
                    delay(1000)
                }
            }
            assertEquals(1000, hits)
        }
    }
}
