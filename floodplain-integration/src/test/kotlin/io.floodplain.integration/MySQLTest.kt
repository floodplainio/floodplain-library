package io.floodplain.integration

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.mysqlSource
import io.floodplain.kotlindsl.mysqlSourceConfig
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import kotlinx.coroutines.delay
import org.junit.After
import org.junit.Ignore
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

class MySQLTest {

    private val mysqlContainer = InstantiatedContainer("debezium/example-mysql:1.2", 3306, mapOf(
        "MYSQL_ROOT_PASSWORD" to "mysecretpassword",
        "MYSQL_DATABASE" to "wpdb",
        "MYSQL_USER" to "mysqluser",
        "MYSQL_PASSWORD" to "mysqlpw",
        "MYSQL_ROOT_HOST" to "%"
    ))
    private val mongoContainer = InstantiatedContainer("mongo:latest", 27017)

    @After
    fun shutdown() {
        mysqlContainer.close()
        mongoContainer.close()
    }

    @Test
    fun testInventory() {
    }
    // can make this a proper unit test when I have a persisted wordpress installation image
    @Test @Ignore
    fun testWordpress() {
        stream {
            val mysqlConfig = mysqlSourceConfig("mysqlsource", "localhost", 3306, "root", "mysecretpassword", "wpdb")
            val mongoConfig = mongoConfig("mongosink", "mongodb://localhost", "@mongodump2")
            mysqlSource("wpdb.wp_posts", mysqlConfig) {
                each { key, msg, other ->
                    logger.info("Detected key: $key and message: $msg")
                }
                mongoSink("posts", "@topicdef", mongoConfig)
            }
        }.renderAndExecute {
            delay(1000000)
        }
    }
}
