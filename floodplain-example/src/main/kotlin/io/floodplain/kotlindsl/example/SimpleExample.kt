package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.*
import java.net.URL
import java.util.*

fun main() = pipe("mygeneration") {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        joinRemote({ msg -> msg["language_id"].toString() }) { postgresSource("public", "language", pgConfig) {} }
        set { film, language ->
            film["language"] = language["name"]; film
        }
        mongoSink("filmwithlanguage", "filmwithlanguage", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())

fun mainold() {
    pipe("mygeneration") {
        val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
        postgresSource("public", "actor", pgConfig) {
            set { msg, _ -> msg["last_update"] = null; msg }
            filter { msg, _ -> (msg["actor_id"] as Int) < 10 }
            mongoSink("mycollection", "sometopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "kafka:9092", UUID.randomUUID().toString())
}
