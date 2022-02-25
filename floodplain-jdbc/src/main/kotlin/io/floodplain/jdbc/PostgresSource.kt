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

private val logger = mu.KotlinLogging.logger {}

private class PostgresConfig(
    override val topologyContext: TopologyContext,
    override val topologyConstructor: TopologyConstructor,
    val name: String,
    val offsetId: String,
    private val hostname: String,
    private val port: Int,
    private val username: String,
    private val password: String,
    private val database: String,
    val defaultSchema: String? = null
) : SourceConfig {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val broadcastFlow = directSource(offsetId)
        broadcastFlow.collect {
            acceptRecord(inputReceiver, it)
        }
        logger.info("connectSource completed")
    }

    private fun acceptRecord(inputReceiver: InputReceiver, record: ChangeRecord) {
        val availableSourceTopics =
            sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString() }.toSet()
        if (availableSourceTopics.contains(record.topic)) {
            if (record.value != null) {
                inputReceiver.input(record.topic, record.key.toByteArray(), record.value!!)
            } else {
                inputReceiver.delete(record.topic, record.key)
            }
        }
    }

    override fun materializeConnectorConfig(): List<MaterializedConfig> {
        return listOf(
            MaterializedConfig(
                name,
                emptyList(),
                mapOf(
                    "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname" to hostname,
                    "database.port" to port.toString(),
                    "database.dbname" to database,
                    "database.user" to username,
                    "database.password" to password,
                    "topic.creation.default.replication.factor" to "1",
                    "topic.creation.default.partitions" to "5",
                    "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    // TODO"table.whitelist": "public.inventory"
                    "public.inventory" to sourceElements.joinToString(",") { e -> "${e.schema()}.${e.table()}" }
                )
            )
        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(offsetId: String): Flow<ChangeRecord> {
        return createDebeziumChangeFlow(
            topologyContext.topicName(name),
            "io.debezium.connector.postgresql.PostgresConnector",
            hostname,
            port,
            database,
            username,
            password,
            offsetId
        )
    }
}

fun Stream.postgresSourceConfig(
    name: String,
    hostname: String,
    port: Int = 5432,
    username: String,
    password: String,
    database: String,
    defaultSchema: String?
): SourceConfig {
    val postgresConfig = PostgresConfig(
        this.topologyContext,
        this.topologyConstructor,
        name,
        topologyContext.applicationId(),
        hostname,
        port,
        username,
        password,
        database,
        defaultSchema
    )
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun PartialStream.postgresSource(
    table: String,
    abstractConfig: SourceConfig,
    schema: String? = null,
    init: Source.() -> Unit
): Source {
    val config = abstractConfig as PostgresConfig
    val effectiveSchema = schema ?: config.defaultSchema
        ?: throw IllegalArgumentException("No schema defined, and also no default schema")
    val topic = Topic.from("${config.name}.$effectiveSchema.$table", topologyContext)
    val topicSource = TopicSource(
        topic,
        Topic.FloodplainKeyFormat.CONNECT_KEY_JSON,
        Topic.FloodplainBodyFormat.CONNECT_JSON
    )
    config.addSourceElement(DebeziumSourceElement(topic, effectiveSchema, table))
    val databaseSource = Source(rootTopology, topicSource, topologyContext)
    databaseSource.init()
    return databaseSource
}

fun Stream.postgresSource(table: String, config: SourceConfig, schema: String? = null, init: Source.() -> Unit) {
    val databaseSource = PartialStream(this).postgresSource(table, config, schema, init)
    addSource(databaseSource)
}

fun Stream.postgresSource(name: String, table: String, schema: String, init: Source.() -> Unit) {
    val topic = Topic.from("$name.$schema.$table", topologyContext)
    val topicSource = TopicSource(
        topic,
        Topic.FloodplainKeyFormat.CONNECT_KEY_JSON,
        Topic.FloodplainBodyFormat.CONNECT_JSON
    )
    val databaseSource = Source(this, topicSource, topologyContext)
    databaseSource.init()
    addSource(databaseSource)
}

class DebeziumSourceElement(private val prefix: Topic, private val schema: String? = null, val table: String) :
    SourceTopic {
    override fun topic(): Topic {
        return prefix
    }

    override fun schema(): String? {
        return schema
    }

    override fun table(): String {
        return table
    }
}
