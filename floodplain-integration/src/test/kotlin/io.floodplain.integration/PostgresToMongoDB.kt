package io.floodplain.integration

import com.mongodb.client.MongoClients
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import kotlin.test.assertEquals
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import org.junit.After
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class TestCombinedMongo {

    val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)
    val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

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
        println("Logger class: ${logger.underlyingLogger}")
        streams("any", "myinstance") {
            val postgresConfig = postgresSourceConfig("mypostgres", postgresContainer.host, postgresContainer.exposedPort, "postgres", "mysecretpassword", "dvdrental", "public")
            val mongoConfig = mongoConfig("mongosink", "mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}", "@mongodump")
            listOf(
                postgresConfig.sourceSimple("address") {
                    joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                        postgresConfig.sourceSimple("city") {
                            joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                                postgresConfig.sourceSimple("country") {}
                            }
                            set { _, msg, state ->
                                msg.set("country", state)
                            }
                        }
                    }
                    set { _, msg, state ->
                        msg.set("city", state)
                    }
                    sink("@address", false)
                    // mongoSink("address", "@address",  mongoConfig)
                },
                postgresConfig.sourceSimple("customer") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("customer", "@customer", mongoConfig)
                },
                postgresConfig.sourceSimple("store") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                    }
                    mongoSink("store", "@store", mongoConfig)
                },
                postgresConfig.sourceSimple("staff") {
                    joinRemote({ m -> "${m["address_id"]}" }, false) {
                        source("@address") {}
                    }
                    set { _, msg, state ->
                        msg.set("address", state)
                        msg["address_id"] = null
                        msg
                    }
                    mongoSink("staff", "@staff", mongoConfig)
                })
        }.renderAndTest {
            logger.info("Outputs: ${outputs()}")
            delay(30000)
            val database = topologyContext().topicName("@mongodump")

            connectJobs().forEach { it.cancel("ciao!") }
            MongoClients.create("mongodb://${mongoContainer.host}:${mongoContainer.exposedPort}").use { client ->
                val collection = client.getDatabase(database)
                    .getCollection("customer")
                // collection.deleteMany(Document())
                var doccount = collection
                    .countDocuments()
                logger.info("Count of Documents: $doccount in database: $database")
                // Thread.sleep(100000)
                assertEquals(599L, doccount)
            }
        }
    }
}
