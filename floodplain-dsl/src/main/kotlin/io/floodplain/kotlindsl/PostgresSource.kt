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
package io.floodplain.kotlindsl

import io.floodplain.ChangeRecord
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.postgresDataSource
import io.floodplain.reactive.source.topology.DebeziumTopicSource
import io.floodplain.reactive.source.topology.TopicSource
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.debezium.JSONToReplicationMessage.processDebeziumBody
import io.floodplain.streams.debezium.JSONToReplicationMessage.processDebeziumJSONKey
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect

private val logger = mu.KotlinLogging.logger {}

class PostgresConfig(val topologyContext: TopologyContext, val name: String, val hostname: String, val port: Int, val username: String, val password: String, val database: String, val defaultSchema: String? = null) : Config {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    // val processor = DebeziumConversionProcessor()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }
    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val elements = sourceElements.toSet()
        val broadcastFlow = directSource(Paths.get("somepath" + UUID.randomUUID().toString()))
        broadcastFlow.collect {
            val availableSourceTopics = sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString(topologyContext) }.toSet()
            if (availableSourceTopics.contains(it.topic)) {
                val parsedKey = processDebeziumJSONKey(it.key)
                // println("matched! Matching key: $parsedKey unparsedKey : ${it.key}")
                // println("value: ${String(it.value)}")
                // inputReceiver.input(it.topic,it.key,it.value)
                val rm = processDebeziumBody(it.value)
                val operation = rm.operation()
                try {
                    when (operation) {
                        ReplicationMessage.Operation.DELETE ->
                            inputReceiver.delete(it.topic, parsedKey)
                        else ->
                            inputReceiver.input(it.topic, parsedKey, fromImmutable(rm.message()))
                    }
                } catch (e: Throwable) {
                    e.printStackTrace()
                }
            }
        }
    }

    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>> {
        return name to mapOf(
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
                // TODO
                "tablewhitelistorsomething" to sourceElements.map { e -> e.topic().qualifiedString(topologyContext) }.joinToString(",")
        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    fun directSource(offsetFilePath: Path): Flow<ChangeRecord> {
        return postgresDataSource(topologyContext.topicName(name), hostname, port, database, username, password, offsetFilePath)
    }

    fun sourceSimple(table: String, schema: String? = null, init: Source.() -> Unit): Source {
        val effectiveSchema = schema ?: defaultSchema ?: "public"
        val topic = Topic.from(name + "." + effectiveSchema + "." + table)
        val topicSource = TopicSource(topic, false)
        addSourceElement(DebeziumSourceElement(topic))
        val databaseSource = Source(topicSource)
        databaseSource.init()
        return databaseSource
    }

    fun source(table: String, schema: String? = null, init: Source.() -> Unit): Source {
        val effectiveSchema = schema ?: defaultSchema ?: "public"
        val topicSource = DebeziumTopicSource(name, table, effectiveSchema)
        addSourceElement(DebeziumSourceElement(topicSource.topic))
        val databaseSource = Source(topicSource)
        databaseSource.init()
        return databaseSource
    }
}

fun Stream.postgresSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String, defaultSchema: String? = null): PostgresConfig {
    val postgresConfig = PostgresConfig(this.context, name, hostname, port, username, password, database, defaultSchema)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

@Deprecated("Use the config object")
fun Stream.postgresSource(schema: String, table: String, config: PostgresConfig, init: Source.() -> Unit): Source {

    val topicSource = DebeziumTopicSource(config.name, table, schema)
    // val topicName = topicSource.topicName(this.context)
    config.addSourceElement(DebeziumSourceElement(topicSource.topic))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    return databaseSource
}

class DebeziumSourceElement(val prefix: Topic) : SourceTopic {
    override fun topic(): Topic {
        return prefix
    }
}
