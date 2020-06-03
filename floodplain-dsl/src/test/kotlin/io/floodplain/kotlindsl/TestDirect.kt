package io.floodplain.kotlindsl

import io.floodplain.postgresDataSource
import io.floodplain.replication.api.ReplicationMessageParser
import io.floodplain.replication.factory.ReplicationFactory
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl
import io.floodplain.streams.debezium.JSONToReplicationMessage
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Optional
import java.util.UUID
import kotlin.test.assertEquals
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import org.testcontainers.containers.GenericContainer

private val logger = mu.KotlinLogging.logger {}

private val parser: ReplicationMessageParser = JSONReplicationMessageParserImpl()

@kotlinx.coroutines.ExperimentalCoroutinesApi
class TestDirect {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var address: String? = "localhost"
    var port: Int? = 5432

    var container: GenericContainer<*>? = null

    init {

        // var envVar = System.getenv("EMBED_CONTAINER")
        var envVar = "aap"
        if (envVar != null) {
            container = KGenericContainer("floodplain/floodplain-postgres-demo:1.0.0")
                .apply { withExposedPorts(5432) }
            container?.start()
            address = container?.getHost()
            port = container?.getFirstMappedPort()
        } else {
            address = "localhost"
            port = 5432
        }
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */

    // @Before
    // fun setup() {
    //     if(false) {
    //         container = GenericContainer<Nothing>("floodplain/floodplain-postgres-demo:1.0.0")
    //             .apply{ withExposedPorts(5432) }
    //         container?.start()
    //         address = container?.getHost()
    //         port = container?.getFirstMappedPort()
    //     } else {
    //         address = "localhost"
    //         port = 5432
    //     }
    // }

    @After
    fun shutdown() {
        container?.close()
    }

    @Test @Ignore
    fun testPostgresSourceJustTheInfra() {
        stream("any", "myinstance") {
            val pgConfig = postgresSourceConfig("mypostgres", "localhost", 5432, "postgres", "mysecretpassword", "dvdrental", "public")
            pgConfig.source("city") {
                sink("topic")
            }
        }.renderAndTest {
            assertEquals(1, this.sourceConfigurations().size)
            val postgresSource = this.sourceConfigurations().first()
            // assertEquals(1, postgresSource.sourceElements().size)
            // val topicSource = postgresSource.sourceElements().first()

            // assertEquals("myinstance-mypostgres.public.city", topicSource.topic().qualifiedString(topologyContext()))
        }
    }

    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
    @Test @Ignore
    fun testPostgresSource() {
        println("Logger class: ${logger.underlyingLogger}")
        logger.debug("startdebug")
        stream("any", "myinstance") {
            val pgConfig = postgresSourceConfig("mypostgres", address!!, port!!, "postgres", "mysecretpassword", "dvdrental", "public")
            pgConfig.sourceSimple("city") {
                filter { _, msg
                    -> msg.string("city").length == 8
                }
                sink("@topic")
            }
        }.renderAndTest {
            logger.info("Detected sinks: ${topologyConstructor().sinks}")
            val jb = GlobalScope.launch {
                    val job = launch { connectSource() }
                    val cities = outputFlow()
                        .take(50)
                        .onCompletion { delay(5000); job.cancel("Cancelling source") }
                        .map { (key, message) -> message.string("city") }
                        .toList()

                println("Join output DONE. Size: ${cities.size} Cities: " + cities.joinToString(","))
                // delay(1000)
                println("Job is active: ${job.isActive}")
            }
            runBlocking {
                logger.info("jb starting. Status: ${jb.isActive}")
                jb.join()
                logger.info("Run blocking complete")
            }
        }
    }
    @Test(expected = CancellationException::class) @Ignore
    fun testPostgresSourceSimple() {
        ReplicationFactory.setInstance(parser)
        val tempPath = Paths.get("path_${UUID.randomUUID()}")
        runBlocking {
            launch {
                postgresDataSource("mypostgres", "localhost", 5432, "dvdrental", "postgres", "mysecretpassword", tempPath)
                    .map {
                        record ->
                        val kv = JSONToReplicationMessage.parse(record.key, record.value)
                        println("<<<<|>>>> ${kv.key}")

                        val msg = parser.parseBytes(Optional.empty<String>(), kv.value)
                        kv.key to msg
                    }
                    .collect {
                        println(it.first)
                    }
            }
            delay(100000)
            cancel("done")
        }
        Files.deleteIfExists(tempPath)
    }

    @Test @Ignore
    fun testPostgresSlowConsume() {
        val path = Paths.get("_someoffsetpath" + UUID.randomUUID())
        val broadcastFlow = postgresDataSource("mypostgres", "localhost", 5432, "dvdrental", "postgres", "mysecretpassword", path)
        runBlocking {
            val jv = launch {
                broadcastFlow.collect {
                    delay(3000)
                    println("Record: ${it.topic} key: ${it.key}")
                }
            }
            delay(10000)
            jv.cancel("bye")
        }
    }
}
