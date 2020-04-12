package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import java.net.URL
import java.util.*

fun main() {
    pipe("mygeneration") {
        val pgConfig = postgresSourceConfig("mypostgres","postgres",5432,"postgres","mysecretpassword","dvdrental")
        val mongoConfig = mongoConfig("mymongo","mongodb://mongo","mydatabase")
        postgresSource("public","actor",pgConfig) {
            mongoSink("mycollection","sometopic",mongoConfig)
        }
    }.renderAndStart(URL( "http://localhost:8083/connectors"),"kafka:9092", UUID.randomUUID().toString())
}
