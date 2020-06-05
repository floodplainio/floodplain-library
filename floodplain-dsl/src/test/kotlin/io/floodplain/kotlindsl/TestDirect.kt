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
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertEquals
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
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
        var envVar = null // "aap"
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
    @Test
    fun testPostgresSource() {
        println("Logger class: ${logger.underlyingLogger}")
        logger.debug("startdebug")
        stream("any", "myinstance") {
            val pgConfig = postgresSourceConfig("mypostgres", address!!, port!!, "postgres", "mysecretpassword", "dvdrental", "public")
            val elasticConfig = elasticSearchConfig("elastic", "http://localhost:9200")

            pgConfig.sourceSimple("city") {
                set { _, msg, _ -> msg["last_update"] = null; msg }
                each { key, iMessage, _ -> logger.info("Keyyyyy: $key $iMessage") }
                // filter { _, msg
                //     -> msg.string("city").length == 8
                // }
                // sink("@topic")
                elasticSearchSink("somelink", "someindex", "@topic", elasticConfig)
            }
        }.renderAndTest {

            val consumer = initializeSinks()
            val jb = GlobalScope.launch {
                val job = launch(EmptyCoroutineContext, CoroutineStart.UNDISPATCHED) { connectSource() }
                outputFlow()
                    .collect {
                        logger.info("Consuming topic: ${it.first}")
                        consumer(it)
                    }
                job.start()
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
