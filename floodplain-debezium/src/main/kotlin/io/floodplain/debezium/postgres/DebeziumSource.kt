/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.debezium.postgres

import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import io.floodplain.ChangeRecord
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import java.nio.file.Path
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempFile
import kotlin.system.measureTimeMillis

private val logger = mu.KotlinLogging.logger {}

private const val OFFSETFILELENGTH = 7
private const val SENDBLOCKING_WARN_THRESHOLD = 1000
internal class EngineKillSwitch(var engine: DebeziumEngine<ChangeEvent<String, String>>? = null) {

    private val killed = AtomicBoolean(false)
    fun kill() {
        engine?.let {
            if (killed.compareAndSet(false, true)) {
                logger.debug("Closing engine: $engine")
                it.close()
            }
        }
    }
}

@OptIn(ExperimentalPathApi::class)
private fun createOffsetFilePath(offsetId: String?): Path {
    val tempFile = createTempFile(offsetId ?: UUID.randomUUID().toString().substring(0..OFFSETFILELENGTH))
    if (offsetId == null) {
        tempFile.toFile().deleteOnExit()
    }
    return tempFile
}

private fun createLocalDebeziumSettings(
    name: String,
    taskClass: String,
    hostname: String,
    port: Int,
    database: String,
    username: String,
    password: String,
    topicPrefix: String,
    serverId: Long,
    offsetId: String? = null,
    settings: Map<String, String> = emptyMap()
): Properties {
    val offsetFilePath = createOffsetFilePath(offsetId)
    logger.info("Creating offset files at: $offsetFilePath")
    val props = Properties()
    props.setProperty("name", "engine_" + UUID.randomUUID())
    props.setProperty("connector.class", taskClass)
    props.setProperty("database.hostname", hostname)
    props.setProperty("database.port", "$port")
    props.setProperty("database.server.name", name) // don't think this matters?
    props.setProperty("database.server.id", serverId.toString()) // don't think this matters?
    props.setProperty("database.dbname", database)
    // props.setProperty("database.include.list", database)
    props.setProperty("database.user", username)
    props.setProperty("database.password", password)
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
    props.setProperty("offset.flush.interval.ms", "1000")
    props.setProperty("topic.prefix", topicPrefix)

    // Override any setting:
    settings.forEach { (k, v) -> props[k] = v }
    return props
}

/**
 * @return A hot flow of ChangeRecord. Perhaps one day there might be a colder one.
 * @param name: The prefix of the outgoing 'topic', basically the destination
 * field of the changerecord is <topicprefix>.<schema>.<table>
 * @param hostname: The host of the postgres database
 * @param port: The port of the postgres database
 * @param username: The username of the postgres database
 * @param password: The password of the postgres database
 * @param offsetId: By default, we will save the offsets in a file path
 * @param settings An optional string-string map, that represents any extra
 * parameters you want to pass to Debezium
 * Defaults to empty map.
 *
 */
fun createDebeziumChangeFlow(
    name: String,
    taskClass: String,
    hostname: String,
    port: Int,
    database: String,
    username: String,
    password: String,
    topicPrefix: String,
    serverId: Long,
    offsetId: String? = null,
    settings: Map<String, String> = emptyMap()
): Flow<ChangeRecord> {
    val props = createLocalDebeziumSettings(
        name, taskClass, hostname, port, database,
        username, password,topicPrefix, serverId, offsetId, settings
    )
    props.list(System.out)
    return runDebeziumServer(props)
}

@Suppress("SwallowedException")
private fun ProducerScope<ChangeRecord>.createEngine(
    props: Properties,
    engineKillSwitch: EngineKillSwitch
): DebeziumEngine<ChangeEvent<String, String>> {
    return DebeziumEngine.create(Json::class.java)
        .using(props)
        .notifying { record: ChangeEvent<String, String> ->
            if (isClosedForSend) {
                // logger.info("Closed for send")
            } else {
                val perf = measureTimeMillis {
                    try {
                        trySendBlocking(
                            ChangeRecord(
                                record.destination(),
                                record.key(),
                                record.value()?.toByteArray()
                            )
                        )
                    } catch (e: CancellationException) {
                        logger.info("engine cancelled")
                        engineKillSwitch.kill()
                        Thread.currentThread().interrupt()
                    }
                }
                if (perf > SENDBLOCKING_WARN_THRESHOLD) {
                    logger.debug("Send blocking ran for: $perf")
                }
            }
        }
        .build()
}

@OptIn(DelicateCoroutinesApi::class)
private fun runDebeziumServer(props: Properties): Flow<ChangeRecord> {
    val engineKillSwitch = EngineKillSwitch()
    return callbackFlow {
        val engine: DebeziumEngine<ChangeEvent<String, String>> = createEngine(props, engineKillSwitch)
        GlobalScope.launch {
            engine.run()
            // logger.info("Debezium source engine terminated. Total time in send: ${totalTimeInSend.get()}")
        }

        engineKillSwitch.engine = engine
        //
        awaitClose {
            engine.close()
        }
        logger.info("engine started")
    }.onCompletion {
        engineKillSwitch.kill()
        logger.info("Debezium flow shutdown completed")
    }
}
