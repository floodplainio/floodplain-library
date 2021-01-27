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
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach

private val logger = mu.KotlinLogging.logger {}

class MySQLConfig(val topologyContext: TopologyContext, val name: String, private val offsetId: String, private val hostname: String, private val port: Int, private val username: String, private val password: String, private val database: String) : SourceConfig {

    private val sourceElements: MutableList<SourceTopic> = mutableListOf()

    override fun sourceElements(): List<SourceTopic> {
        return sourceElements
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
        val broadcastFlow = directSource(offsetId)
        broadcastFlow
            .onEach { logger.info("Record found: ${it.topic} ${it.key}") }
            .collect {
                val availableSourceTopics = sourceElements.map { sourceElement -> sourceElement.topic().qualifiedString() }.toSet()
                if (availableSourceTopics.contains(it.topic)) {
                    if (it.value != null) {
                        inputReceiver.inputQualified(it.topic, it.key.toByteArray(), it.value!!)
                    } else {
                        inputReceiver.deleteQualified(it.topic, it.key)
                    }
                }
            }
        logger.info("connectSource completed")
    }

    override fun materializeConnectorConfig(topologyContext: TopologyContext): List<MaterializedConfig> {
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
                    "database.whitelist" to database,
                    "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                    // TODO Deal with this, is it required?
                    "database.history.kafka.bootstrap.servers" to "kafka:9092",
                    "database.history.kafka.topic" to "dbhistory.wordpress",
                    "include.schema.changes" to "false"
                )
            )
        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    private fun directSource(offsetId: String): Flow<ChangeRecord> {
        val tempFile = createTempFile(offsetId)

        val extraSettings = mapOf(
            "database.history" to "io.debezium.relational.history.FileDatabaseHistory",
            "database.history.file.filename" to tempFile.absolutePath
        )
        return createDebeziumChangeFlow(topologyContext.topicName(name), "io.debezium.connector.mysql.MySqlConnector", hostname, port, database, username, password, offsetId, extraSettings)
            .onCompletion { e ->
                if (e != null) {
                    logger.info("Error in debezium source", e)
                }
                tempFile.delete()
            }
    }
}

fun Stream.mysqlSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String): MySQLConfig {
    val mySQLConfig = MySQLConfig(this.topologyContext, name, topologyContext.applicationId(), hostname, port, username, password, database)
    addSourceConfiguration(mySQLConfig)
    return mySQLConfig
}

fun PartialStream.mysqlSource(table: String, config: MySQLConfig, init: Source.() -> Unit): Source {
    val topic = Topic.from("${config.name}.$table", topologyContext)
    val topicSource = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    config.addSourceElement(DebeziumSourceElement(topic, null, table))
    val databaseSource = Source(rootTopology, topicSource, topologyContext)
    databaseSource.init()
    return databaseSource
}

fun Stream.mysqlSource(table: String, config: MySQLConfig, init: Source.() -> Unit) {
    addSource(PartialStream(this).mysqlSource(table, config, init))
}
