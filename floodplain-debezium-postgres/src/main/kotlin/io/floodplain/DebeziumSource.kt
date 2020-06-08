package io.floodplain

import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.system.measureTimeMillis
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.runBlocking

private val logger = mu.KotlinLogging.logger {}

data class ChangeRecord(val topic: String, val key: String, val value: ByteArray?)

fun main(args: Array<String>) {
    val offsetFilePath = Paths.get("offset")
    Files.deleteIfExists(offsetFilePath)
    runBlocking {
        postgresDataSource("dvdrental", "localhost", 5432, "dvdrental", "postgres", "mysecretpassword", offsetFilePath)

            .collect() { record ->
                // val kv = JSONToReplicationMessage.parse(record.key, record.value.toByteArray())
                // val msg = parser.parseBytes(Optional.empty<String>(), kv.value)

                println("item: ${record.topic} : ${record.key} : ${record.value}")
            }
    }
}

/**
 * @return A hot flow of ChangeRecord. Perhaps one day there might be a colder one.
 * @param name: The prefix of the outgoing 'topic', basically the destination field of the changerecord is <topicprefix>.<schema>.<table>
 * @param hostname: The host of the postgres database
 * @param port: The port of the postgres database
 * @param username: The username of the postgres database
 * @param password: The password of the postgres database
 * @param offsetFilePath: By default, we will save the offsets in a file path
 * @param settings An optional string-string map, that represents any extra parameters you want to pass to Debezium
 * Defaults to empty map.
 *
 */

internal class EngineKillSwitch(var engine: DebeziumEngine<ChangeEvent<String, String>>? = null) {

    val killed = AtomicBoolean(false)
    fun kill() {
        engine?.let {
            if (killed.compareAndSet(false, true)) {
                println("Closing engine: $engine")
                it.close()
            }
        }
    }
}
@kotlinx.coroutines.ExperimentalCoroutinesApi
fun postgresDataSource(name: String, hostname: String, port: Int, database: String, user: String, password: String, offsetFilePath: Path, settings: Map<String, String> = emptyMap()): Flow<ChangeRecord> {
        val props = Properties()
    val startedAt = System.currentTimeMillis()
        props.setProperty("name", "engine_" + UUID.randomUUID())
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.hostname", hostname)
        props.setProperty("database.port", "$port")
        props.setProperty("database.server.name", name) // don't think this matters?
        props.setProperty("database.dbname", database)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.user", user)
        props.setProperty("database.password", password)
        props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
        props.setProperty("offset.flush.interval.ms", "10000")
        settings.forEach { k, v -> props[k] = v }
        logger.info("Starting source flow with name: ${props.getProperty("name")} offsetpath: $offsetFilePath ")
        val engineKillSwitch = EngineKillSwitch()
        return callbackFlow<ChangeRecord> {
            val engine = DebeziumEngine.create(Json::class.java)
                .using(props)
                .notifying { record: ChangeEvent<String, String> ->
                    // if (isActive) {
                        val perf = measureTimeMillis {
                            try {
                                sendBlocking(ChangeRecord(record.destination(), record.key(), record.value()?.toByteArray()))
                            } catch (e: CancellationException) {
                                engineKillSwitch.kill()
                                Thread.currentThread().interrupt()
                            }
                        }
                        if (perf > 1000) {
                            println("Send blocking ran for: " + perf)
                        }

                    // }
                }
                .build()
            // TODO I'm unsure if it is wise to run this in the same thread
            // Seems to work fine for my tests, so for the time being I'll keep it simple.
            engineKillSwitch.engine = engine
            println("Engine ran for: " + measureTimeMillis {
                engine.run() })
            // engine.run()
            println("Engine run completed")
            // Thread {
            // }.start()
            // engine.run()
            awaitClose {
                println("closin!")
                engine.close()
            }
        }
        .onCompletion { engineKillSwitch.kill() }
    }
