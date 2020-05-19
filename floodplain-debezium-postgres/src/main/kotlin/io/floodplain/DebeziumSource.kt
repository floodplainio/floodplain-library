package io.floodplain

import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Properties
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking

private val logger = mu.KotlinLogging.logger {}

data class ChangeRecord(val topic: String, val key: String, val value: String)

fun main(args: Array<String>) {
    val offsetFilePath = "offset"
    Files.deleteIfExists(Paths.get(offsetFilePath))
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
 * @param topicPrefix: The prefix of the outgoing 'topic', basically the destination field of the changerecord is <topicprefix>.<schema>.<table>
 * @param hostname: The host of the postgres database
 * @param port: The port of the postgres database
 * @param username: The username of the postgres database
 * @param password: The password of the postgres database
 * @param offsetFilePath: By default, we will save the offsets in a file path
 * @param settings An optional string-string map, that represents any extra parameters you want to pass to Debezium
 * Defaults to empty map.
 *
 */
fun postgresDataSource(topicPrefix: String, hostname: String, port: Int, database: String, user: String, password: String, offsetFilePath: String, settings: Map<String, String> = emptyMap()): Flow<ChangeRecord> {
        val props = Properties()
        props.setProperty("name", "engine2")
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.hostname", hostname)
        props.setProperty("database.port", "$port")
        props.setProperty("database.server.name", topicPrefix) // don't think this matters?
        props.setProperty("database.dbname", database)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.user", user)
        props.setProperty("database.password", password)
        props.setProperty("offset.storage.file.filename", offsetFilePath)
        props.setProperty("offset.flush.interval.ms", "10000")
        settings.forEach { k, v -> props.put(k, v) }
        logger.info("Starting source flow with name: ${props.getProperty("name")} offsetpath: $offsetFilePath ")
        return callbackFlow<ChangeRecord> {
            val engine = DebeziumEngine.create(Json::class.java)
                .using(props)
                .notifying { record: ChangeEvent<String, String> ->
                    sendBlocking(ChangeRecord(record.destination(), record.key(), record.value()))
                }
                .build()
            // ewww TODO
            Thread {
                engine.run()
            }.start()
            awaitClose {
                println("closin!")
                engine.close()
            }
        }
    }
