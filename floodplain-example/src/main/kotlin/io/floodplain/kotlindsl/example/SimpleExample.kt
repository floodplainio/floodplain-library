package io.floodplain.kotlindsl.example

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.*
import java.net.URL
import java.util.*

fun main() {

//    postgresql://user:secret@localhost

    val tenant = "mytenant"
    val deployment = "mydeployment"
    val instance = "myinstance"
    val generation = "mygeneration"
    var topologyContext = TopologyContext(Optional.ofNullable(tenant), deployment, instance, generation)
    var topologyConstructor = TopologyConstructor(Optional.empty())


    pipe(topologyContext,topologyConstructor) {
        val postgresConfig = postgresSourceConfig("mypostgres","postgres",5432,"postgres","mysecretpassword","dvdrental")
        val mongoConfig = mongoConfig("mymongo","mongodb://mongo","mydatabase")
        debeziumSource("public","actor",postgresConfig) {
            mongoSink(topologyContext,"mycollection","sometopic",mongoConfig)
        }
    }.renderAndStart()
//    sources.forEach { (name, json) ->
//        startConstructor(name,topologyContext, URL( "http://localhost:8083/connectors"),json,true  )
//    }
//    Thread.sleep(5000)
//    sinks.forEach { (name, json) ->
//        startConstructor(name,topologyContext, URL( "http://localhost:8083/connectors"),json,true  )
//    }
//    runTopology(topology,"appId","kafka:9092","storagePath")

//    runTopology(topology,"app","kafka:9092","storage")
}
