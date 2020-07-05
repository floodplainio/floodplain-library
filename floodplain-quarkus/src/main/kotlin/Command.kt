import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import org.apache.kafka.streams.KafkaStreams

private val logger = mu.KotlinLogging.logger {}

@QuarkusMain
class Command : QuarkusApplication {
    override fun run(vararg args: String?): Int {
        // val a: io.debezium.engine.format.Json
        // val b: ReplicationMessage
        val a: KafkaStreams? = null
        println("do something")
        streams {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                "localhost",
                5432,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://localhost:27017",
                "@mongodump"
            )
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
            waitUntilCtrlC()
        }
        println("done!")
        return 0
    }

    private fun waitUntilCtrlC() {
        val thread = Thread.currentThread()
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                println("CTRLC detected!")
                thread.interrupt()
            }
        })
        var interrupted = false
        while (!interrupted) {
            try {
                Thread.sleep(3000000)
            } catch (e: InterruptedException) {
                // ignore
                interrupted = true
            }
        }
    }
}
