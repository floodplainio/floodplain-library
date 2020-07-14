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
import io.floodplain.debezium.postgres.postgresDataSource
import io.floodplain.reactive.source.topology.TopicSource
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect

private val logger = mu.KotlinLogging.logger {}

class PostgresConfig(val topologyContext: TopologyContext, val name: String, val offsetId: String, private val hostname: String, private val port: Int, private val username: String, private val password: String, private val database: String, private val defaultSchema: String? = null) : Config {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val broadcastFlow = directSource(offsetId)
        broadcastFlow.collect {
            val availableSourceTopics = sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString(topologyContext) }.toSet()
            if (availableSourceTopics.contains(it.topic)) {
                if (it.value != null) {
                    inputReceiver.input(it.topic, it.key.toByteArray(), it.value!!)
                } else {
                    inputReceiver.delete(it.topic, it.key)
                }
                // val rm: ReplicationMessage? = processDebeziumBody(it.value)
                // val operation = rm?.operation() ?: ReplicationMessage.Operation.DELETE
                // try {
                //     when (operation) {
                //         ReplicationMessage.Operation.DELETE ->
                //             inputReceiver.delete(it.topic, parsedKey)
                //         else ->
                //             inputReceiver.input(it.topic, parsedKey, fromImmutable(rm!!.message()))
                //     }
                // } catch (e: Throwable) {
                //     error("Failed with exception $e")
                // }
            }
        }
        logger.info("connectSource completed")
    }

    override fun sinkTask(): Any? {
        return null
    }

    override fun instantiateSinkElements(topologyContext: TopologyContext) {
        TODO("Not yet implemented")
    }

    override fun sinkElements(): Map<Topic, MutableList<FloodplainSink>> {
        TODO("Not yet implemented")
    }

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedSink> {
        return listOf(MaterializedSink(name, emptyList(), mapOf(
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                // TODO
                "tablewhitelistorsomething" to sourceElements.map { e -> e.topic().qualifiedString(topologyContext) }.joinToString(",")
        )))
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(offsetId: String): Flow<ChangeRecord> {
        return postgresDataSource(topologyContext.topicName(name), hostname, port, database, username, password, offsetId)
    }

    fun sourceSimple(table: String, schema: String? = null, init: Source.() -> Unit): Source {
        val effectiveSchema = schema ?: defaultSchema ?: "public"
        val topic = Topic.from("$name.$effectiveSchema.$table")
        val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.FLOODPLAIN_STRING, Topic.FloodplainBodyFormat.FLOODPLAIN_JSON)
        addSourceElement(DebeziumSourceElement(topic))
        val databaseSource = Source(topicSource)
        databaseSource.init()
        return databaseSource
    }

    fun source(table: String, schema: String? = null, init: Source.() -> Unit): Source {
        val effectiveSchema = schema ?: defaultSchema ?: "public"
        val topic = Topic.from("$name.$effectiveSchema.$table")
        val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
        addSourceElement(DebeziumSourceElement(topic))
        val databaseSource = Source(topicSource)
        databaseSource.init()
        return databaseSource
    }
}

fun Stream.postgresSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String, defaultSchema: String? = null): PostgresConfig {
    val postgresConfig = PostgresConfig(this.context, name, context.applicationId(), hostname, port, username, password, database, defaultSchema)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun Stream.postgresSource(schema: String, table: String, config: PostgresConfig, init: Source.() -> Unit): Source {
    val topic = Topic.from("${config.name}.$schema.$table")
    val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    config.addSourceElement(DebeziumSourceElement(topic))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    return databaseSource
}

class DebeziumSourceElement(val prefix: Topic) : SourceTopic {
    override fun topic(): Topic {
        return prefix
    }
}
