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
import io.floodplain.debezium.postgres.createDebeziumChangeFlow
import io.floodplain.reactive.source.topology.TopicSource
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import java.lang.IllegalArgumentException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect

private val logger = mu.KotlinLogging.logger {}

class PostgresConfig(val topologyContext: TopologyContext, val name: String, val offsetId: String, private val hostname: String, private val port: Int, private val username: String, private val password: String, private val database: String, val defaultSchema: String? = null) : SourceConfig {

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
        val availableSourceTopics = sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString(topologyContext) }.toSet()
        if (availableSourceTopics.contains(record.topic)) {
            if (record.value != null) {
                inputReceiver.input(record.topic, record.key.toByteArray(), record.value!!)
            } else {
                inputReceiver.delete(record.topic, record.key)
            }
        }
    }

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedConfig> {
        return listOf(MaterializedConfig(name, emptyList(), mapOf(
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                // TODO"table.whitelist": "public.inventory"
                "public.inventory" to sourceElements.map { e -> "${e.schema()}.${e.table()}" }.joinToString(",")
        )))
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(offsetId: String): Flow<ChangeRecord> {
        return createDebeziumChangeFlow(topologyContext.topicName(name), "io.debezium.connector.postgresql.PostgresConnector", hostname, port, database, username, password, offsetId)
    }
}

fun Stream.postgresSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String, defaultSchema: String?): PostgresConfig {
    val postgresConfig = PostgresConfig(this.context, name, context.applicationId(), hostname, port, username, password, database, defaultSchema)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun Stream.postgresSource(table: String, config: PostgresConfig, schema: String? = null, init: Source.() -> Unit): Source {
    val effectiveSchema = schema ?: config.defaultSchema
    if (effectiveSchema == null) {
        throw IllegalArgumentException("No schema defined, and also no default schema")
    }
    val topic = Topic.from("${config.name}.$effectiveSchema.$table")
    val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    config.addSourceElement(DebeziumSourceElement(topic, effectiveSchema, table))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    return databaseSource
}

class DebeziumSourceElement(val prefix: Topic, val schema: String? = null, val table: String) : SourceTopic {
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
