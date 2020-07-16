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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach

private val logger = mu.KotlinLogging.logger {}

class MySQLConfig(val topologyContext: TopologyContext, val name: String, val offsetId: String, private val hostname: String, private val port: Int, private val username: String, private val password: String, private val database: String, val defaultSchema: String? = null) : Config {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val broadcastFlow = directSource(offsetId)
        broadcastFlow
            .onEach { logger.info("Record found: ${it.topic} ${it.key}") }
            .collect {
            val availableSourceTopics = sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString(topologyContext) }.toSet()
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
                "connector.class" to "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
                "database.server.name" to name,
                "database.whitelist" to database,
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "database.history.kafka.bootstrap.servers" to "kafka:9092",
                "database.history.kafka.topic" to "dbhistory.wordpress",
                "include.schema.changes" to "false"
        // TODO
                // "tablewhitelistorsomething" to sourceElements.map { e -> e.topic().qualifiedString(topologyContext) }.joinToString(",")
        )))
    }

    // val props = Properties()
    // props.setProperty("name", "engine_" + UUID.randomUUID())
    // props.setProperty("connector.class", taskClass)
    // props.setProperty("database.hostname", hostname)
    // props.setProperty("database.port", "$port")
    // props.setProperty("database.server.name", name) // don't think this matters?
    // props.setProperty("database.dbname", database)
    // props.setProperty("database.whitelist", database)
    // props.setProperty("database.user", username)
    // props.setProperty("database.password", password)
    // props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
    // props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
    // props.setProperty("offset.flush.interval.ms", "1000")
    // settings.forEach { (k, v) -> props[k] = v }
    // return props

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(offsetId: String): Flow<ChangeRecord> {
        val historyTopic = Topic.from("@historytopic")
        val extraSettings = mapOf(
            // "database.history.kafka.topic" to historyTopic.qualifiedString(topologyContext),
            "database.history" to "io.debezium.relational.history.FileDatabaseHistory",
            "database.history.file.filename" to "currenthistory"
            // "database.history.kafka.bootstrap.servers" to "something:9092"
        )
        return createDebeziumChangeFlow(topologyContext.topicName(name), "io.debezium.connector.mysql.MySqlConnector", hostname, port, database, username, password, offsetId, extraSettings)
    }
}

fun Stream.mysqlSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String): MySQLConfig {
    val mySQLConfig = MySQLConfig(this.context, name, context.applicationId(), hostname, port, username, password, database)
    addSourceConfiguration(mySQLConfig)
    return mySQLConfig
}

fun Stream.mysqlSource(table: String, config: MySQLConfig, schema: String? = null, init: Source.() -> Unit): Source {

    val topic = Topic.from("${config.name}.$table")
    val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    config.addSourceElement(DebeziumSourceElement(topic))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    return databaseSource
}
