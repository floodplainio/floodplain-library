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
import io.floodplain.postgresDataSource
import io.floodplain.reactive.source.topology.DebeziumTopicSource
import io.floodplain.streams.api.TopologyContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.broadcastIn
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.onEach

private val logger = mu.KotlinLogging.logger {}

class PostgresConfig(val topologyContext: TopologyContext, val name: String, val hostname: String, val port: Int, val username: String, val password: String, val database: String, val defaultSchema: String? = null) : Config() {

    private val sourceElements: MutableList<DebeziumSourceElement> = mutableListOf()
    private var broadcastScope: CoroutineScope? = null

    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>> {
        return name to mapOf(
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
                // TODO
                "tablewhitelistorsomething" to sourceElements.map { e -> e.schema + "." + e.table }.joinToString(",")
        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }

    fun directSource(offsetFilePath: String): Flow<ChangeRecord> {
        return postgresDataSource(topologyContext.topicName(name), hostname, port, database, username, password, offsetFilePath)
    }

    override fun closeSource() {
        broadcastScope?.cancel("doei")
    }
    override fun allSources(coroutineScope: CoroutineScope, offsetFilePath: String): Map<String, Flow<ChangeRecord>> {
        val sourceFlow = directSource(offsetFilePath)
            .onEach {
                record -> logger.info("Found key: ${record.key} for destination: ${record.topic}")
            }
        // cancel previous scopes?
        val broadcast = sourceFlow.broadcastIn(coroutineScope).asFlow()

        return sourceElements.map {
            val downstream = broadcast
                .filter { e -> e.topic == "${it.prefix}.${it.schema}.${it.table}" }
                .onEach { e -> logger.info("Checked if topic ${e.topic} matches expected: ${it.prefix}.${it.schema}.${it.table}") }
            Pair(it.combinedName(), downstream)
        }.toMap()
    }

    fun source(table: String, schema: String? = null, init: Source.() -> Unit): Source {
        val effectiveSchema = schema ?: defaultSchema ?: "public"
        val topicSource = DebeziumTopicSource(name, table, effectiveSchema)
        val topicName = topicSource.topicName(topologyContext)
        addSourceElement(DebeziumSourceElement(topicName, table, effectiveSchema))
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
    val topicName = topicSource.topicName(this.context)
    config.addSourceElement(DebeziumSourceElement(topicName, table, schema))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    return databaseSource
}

class DebeziumSourceElement(val prefix: String, val table: String, val schema: String) {
    fun combinedName(): String {
        return "$prefix.$schema.$table"
    }
}
