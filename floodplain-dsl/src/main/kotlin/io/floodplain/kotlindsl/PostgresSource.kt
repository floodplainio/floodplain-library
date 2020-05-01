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

fun Pipe.postgresSourceConfig(name: String, hostname: String, port: Int, username: String, password: String, database: String): PostgresConfig {
    val postgresConfig = PostgresConfig(name, hostname, port, username, password, database)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun Pipe.postgresSource(schema: String, table: String, config: PostgresConfig, init: Source.() -> Unit): Source {

    val topicSource = DebeziumTopicSource(config.name, table, schema, true, true)
    val topicName = topicSource.topicName(this.context)
    config.addSourceElement(DebeziumSourceElement(topicName, table, schema))
    val databaseSource = Source(topicSource)
    databaseSource.init()
//    this.addSource(databaseSource)
    return databaseSource
}

class DebeziumSourceElement(val topic: String, val table: String, val schema: String)
