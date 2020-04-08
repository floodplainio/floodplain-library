package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.navajo.reactive.source.topology.DebeziumTopic
import com.dexels.navajo.reactive.source.topology.SinkTransformer
import java.util.*


//<postgres.source name="dvd" connector.class="io.debezium.connector.postgresql.PostgresConnector"
//database.hostname="postgres" database.port="5432" database.user="postgres"
//database.password="mysecretpassword" database.dbname="dvdrental"/>

class PostgresConfig(val name: String, val hostname: String, val port: Int, val username: String, val password: String, val database: String): Config() {

    private val sourceElements: MutableList<DebeziumSourceElement> = mutableListOf()

//    mytenant-mydeployment-mypostgres.public.actor
    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String,Map<String,String>> {
        return name to mapOf(
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname" to hostname,
                "database.port" to port.toString(),
                "database.dbname" to database,
                "database.user" to username,
                "database.password" to password,
//                "transforms" to "Reroute",
//                "transforms.Reroute.type" to "io.debezium.transforms.ByLogicalTableRouter",
//                "transforms.Reroute.topic.regex" to "(.*)\\.(.*)\\.(.*)",
//                "transforms.Reroute.topic.regex" to "(.)*",
//                "transforms.Reroute.topic.replacement" to "(.)*",
//                "transforms.Reroute.topic.replacement" to "$2-$1-$4-$3-v2",

//                "databasetopic" to sourceElements.map { e->e.topic }.joinToString ( "," ),
                "tablewhitelistorsomething" to sourceElements.map { e->e.schema+"."+e.table }.joinToString ( "," )

        )
    }

    fun addSourceElement(elt: DebeziumSourceElement) {
        sourceElements.add(elt)
    }
}


fun Pipe.postgresSourceConfig(name: String, hostname: String,port: Int, username: String, password: String, database: String): PostgresConfig {
    val postgresConfig = PostgresConfig(name, hostname, port, username, password, database)
    addSourceConfiguration(postgresConfig)
    return postgresConfig
}

fun Pipe.debeziumSource(schema: String, table: String,config: PostgresConfig, init: Source.() -> Unit): Source {

    val topicSource = DebeziumTopic(config.name, table, schema, true, true)
    val topicName = topicSource.topicName(this.context)
    config.addSourceElement(DebeziumSourceElement(topicName,table,schema))
    val databaseSource = Source(topicSource)
    databaseSource.init()
    this.addSource(databaseSource)
    return databaseSource
}

//fun PartialPipe.postgresSource(topic: String, connectorName: String, config: PostgresConfig) {
//
//    val source = DebeziumTopic()
//    // TODO should be source?
//    val sink = SinkTransformer(topic, Optional.empty(), Optional.of(connectorName), Optional.empty(), configMap )
//    config.addSink(sink)
//    addTransformer(Transformer(sink))
//}
class DebeziumSourceElement(val topic: String, val table: String, val schema: String)
