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
import java.nio.file.Path
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.measureTimeMillis
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch

private val logger = mu.KotlinLogging.logger {}

internal class EngineKillSwitch(var engine: DebeziumEngine<ChangeEvent<String, String>>? = null) {

    private val killed = AtomicBoolean(false)
    fun kill() {
        engine?.let {
            if (killed.compareAndSet(false, true)) {
                println("Closing engine: $engine")
                it.close()
            }
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
fun postgresDataSource(name: String, hostname: String, port: Int, database: String, username: String, password: String, offsetFilePath: Path, settings: Map<String, String> = emptyMap()): Flow<ChangeRecord> {
        val props = Properties()
        props.setProperty("name", "engine_" + UUID.randomUUID())
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.hostname", hostname)
        props.setProperty("database.port", "$port")
        props.setProperty("database.server.name", name) // don't think this matters?
        props.setProperty("database.dbname", database)
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("database.user", username)
        props.setProperty("database.password", password)
        props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
        props.setProperty("offset.flush.interval.ms", "10000")
        settings.forEach { (k, v) -> props[k] = v }
        logger.info("Starting source flow with name: ${props.getProperty("name")} offsetpath: $offsetFilePath ")
        val engineKillSwitch = EngineKillSwitch()
        val totalTimeInSend = AtomicLong(0L)
        return callbackFlow<ChangeRecord> {
            val engine = DebeziumEngine.create(Json::class.java)
                .using(props)
                .notifying { record: ChangeEvent<String, String> ->
                    val perf = measureTimeMillis {
                        try {
                            sendBlocking(
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
                    if (perf > 1000) {
                        println("Send blocking ran for: $perf")
                    }
                    totalTimeInSend.addAndGet(perf)
                }
                .build()
            // TODO I'm unsure if it is wise to run this in the same thread
            // Seems to work fine for my tests, so for the time being I'll keep it simple.
            engineKillSwitch.engine = engine
            GlobalScope.launch {
                println("Engine ran for: " + measureTimeMillis {
                    engine.run() })
                println("Debezium source engine terminated. Total time in send: ${totalTimeInSend.get()}")
            }
            awaitClose {
                println("closin!")
                engine.close()
            }
        }
        .onCompletion { engineKillSwitch.kill() }
    }
