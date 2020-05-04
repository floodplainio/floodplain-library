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

import io.floodplain.reactive.source.topology.DebeziumTopicSource
import io.floodplain.streams.api.TopologyContext

class PostgresConfig(val name: String, val hostname: String, val port: Int, val username: String, val password: String, val database: String) : Config() {

    private val sourceElements: MutableList<DebeziumSourceElement> = mutableListOf()

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
}

fun Stream.postgresSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String): PostgresConfig {
    val postgresConfig = PostgresConfig(name, hostname, port, username, password, database)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun Stream.postgresSource(schema: String, table: String, config: PostgresConfig, init: Source.() -> Unit): Source {

    val topicSource = DebeziumTopicSource(config.name, table, schema, true, true)
    val topicName = topicSource.topicName(this.context)
    config.addSourceElement(DebeziumSourceElement(topicName, table, schema))
    val databaseSource = Source(topicSource)
    databaseSource.init()
//    this.addSource(databaseSource)
    return databaseSource
}

class DebeziumSourceElement(val topic: String, val table: String, val schema: String)
