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
package io.floodplain.jdbc

import io.floodplain.ChangeRecord
import io.floodplain.debezium.postgres.createDebeziumChangeFlow
import io.floodplain.kotlindsl.InputReceiver
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.Source
import io.floodplain.kotlindsl.SourceConfig
import io.floodplain.kotlindsl.SourceTopic
import io.floodplain.kotlindsl.Stream
import io.floodplain.reactive.source.topology.TopicSource
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach

private val logger = mu.KotlinLogging.logger {}

private class MySQLConfig(
    override val topologyContext: TopologyContext,
    override val topologyConstructor: TopologyConstructor,
    val name: String,
    private val offsetId: String,
    private val hostname: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    private val database: String,
    private val topicPrefix: String,
    private val serverId: Long
) : SourceConfig {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val broadcastFlow = directSource(serverId, offsetId,topicPrefix)
        broadcastFlow
            .onEach { logger.info("Record found: ${it.topic} ${it.key}") }
            .collect {
                val availableSourceTopics = sourceElements
                    .map { sourceElement -> sourceElement.topic().qualifiedString() }.toSet()
                if (availableSourceTopics.contains(it.topic)) {
                    if (it.value != null) {
                        inputReceiver.input(it.topic, it.key.toByteArray(), it.value!!)
                    } else {
                        inputReceiver.delete(it.topic, it.key)
                    }
                }
            }
        logger.info("connectSource completed")
    }

    override fun materializeConnectorConfig(): List<MaterializedConfig> {
        return listOf(
            MaterializedConfig(
                name,
                emptyList(),
                mapOf(
                    "connector.class" to "io.debezium.connector.mysql.MySqlConnector",
                    "database.hostname" to hostname,
                    "database.port" to port.toString(),
                    "database.dbname" to database,
                    "database.user" to username,
                    "database.password" to password,
                    "database.server.name" to name,
                    "database.server.id" to serverId.toString(),
                    "database.whitelist" to database,
                    "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    // TODO Deal with this, is it required?
                    "database.history.kafka.bootstrap.servers" to "kafka:9092",
                    "database.history.kafka.topic" to "history",
                    "schema.history.internal.kafka.topic" to "schema_history",
                    "schema.history.internal.kafka.bootstrap.servers" to "kafka:9092",
                    "include.schema.changes" to "false",
                )
            )
        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(serverId: Long, offsetId: String, topicPrefix: String): Flow<ChangeRecord> {
        val tempFile = createTempFile(offsetId)
        val schemaTempFile = createTempFile(offsetId,"_schema")

        val extraSettings = mapOf(
            "database.history" to "io.debezium.relational.history.FileDatabaseHistory",
            "database.history.file.filename" to tempFile.absolutePath,
            "schema.history.internal" to "io.debezium.storage.file.history.FileSchemaHistory",
            "schema.history.internal.file.filename" to schemaTempFile.absolutePath,
        )
        return createDebeziumChangeFlow(
            topologyContext.topicName(name),
            "io.debezium.connector.mysql.MySqlConnector",
            hostname,
            port,
            database,
            username,
            password,
            topicPrefix,
            serverId,
            offsetId,
            extraSettings
        )
            .onCompletion { e ->
                if (e != null) {
                    logger.info("Error in debezium source", e)
                }
                tempFile.delete()
            }
    }
}

fun Stream.mysqlSourceConfig(
    name: String,
    hostname: String,
    port: Int,
    username: String,
    password: String,
    database: String,
    topicPrefix: String,
    serverId: Long = 1
): SourceConfig {
    val mySQLConfig = MySQLConfig(
        this.topologyContext,
        this.topologyConstructor,
        name,
        topologyContext.applicationId(),
        hostname,
        port,
        username,
        password,
        database,
        topicPrefix,
        serverId
    )
    addSourceConfiguration(mySQLConfig)
    return mySQLConfig
}

fun PartialStream.mysqlSource(table: String, abstractConfig: SourceConfig, init: Source.() -> Unit): Source {
    val config = abstractConfig as MySQLConfig
    val topic = Topic.fromQualified("${config.name}.$table", topologyContext)
    val topicSource = TopicSource(
        topic,
        Topic.FloodplainKeyFormat.CONNECT_KEY_JSON,
        Topic.FloodplainBodyFormat.CONNECT_JSON
    )
    config.addSourceElement(DebeziumSourceElement(topic, null, table))
    val databaseSource = Source(rootTopology, topicSource, topologyContext)
    databaseSource.init()
    return databaseSource
}

fun Stream.mysqlSource(table: String, config: SourceConfig, init: Source.() -> Unit) {
    addSource(PartialStream(this).mysqlSource(table, config, init))
}
