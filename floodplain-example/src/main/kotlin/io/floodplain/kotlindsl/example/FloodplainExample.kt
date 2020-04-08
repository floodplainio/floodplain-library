package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import io.floodplain.kotlindsl.message.*
import java.util.*
import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import java.net.URL


fun main() {
    val tenant = "mytenant"
    val deployment = "mydeployment"
    val instance = "myinstance"
    val generation = "mygeneration"
    var topologyContext = TopologyContext(Optional.ofNullable(tenant), deployment, instance, generation)
    var topologyConstructor = TopologyConstructor(Optional.empty())

    val myPipe = pipe(topologyContext, topologyConstructor) {

        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mongosink","mongodb://mongo", "mongodump")
        debeziumSource("public", "customer", postgresConfig) {
            joinWith {
                debeziumSource("public", "payment",postgresConfig) {
                    scan({ msg -> msg["customer_id"].toString() }, { empty().set("total", 0.0) },
                            { set { msg, state -> state["total"] = state["total"] as Double + msg["amount"] as Double; state } },
                            { set { msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state } }
                    )
                    set { _, state ->
                        val result = empty();
                        result["total"] ?: state["total"];
                        result["customer_id"]=state["customer_id"] as Int;
                        result
                    }
                }
            }
            set { msg, state ->
                msg.set("total", state.integer("total"))
            }

            mongoSink(topologyContext,"customerwithtotal", "myfinaltopic", mongoConfig)
        }
    }
    myPipe.renderAndStart()

}


//            .map { e->e.materializeConnectorConfig(topologyContext).forEach(f-) {e.materializeConnectorConfig(topologyContext) })
//    startConnector(topologyContext, URL("http:localhost:8083/connectors"))

    //    fun startConnector(topologyContext: TopologyContext, connectURL: URL, connectorName: String, force: Boolean, parameters: Map<String, Any>) {

//}