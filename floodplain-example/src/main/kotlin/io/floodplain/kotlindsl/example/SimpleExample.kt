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
package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun mainaaa() {
    stream("nextgen7") {
        val mongodb = mongoConfig("@mongo", "mongodb://mongo", "mydatabase")
        val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        postgresSource("public", "actor", pgConfig) {
            each {
                    _, msg, _ -> logger.info("Record: $msg")
            }
            set {
                _, msg, _ -> msg.clear("last_update"); msg
            }
            mongoSink("actorz", "actorz", mongodb)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")
}

fun mainold2() {
    stream {
        val postgres = postgresSourceConfig("local", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongodb = mongoConfig("mongo", "mongodb://mongo", "mydatabase")
        postgresSource("public", "payment", postgres) {
            each {
                    _, msg, _ -> logger.info("Record: $msg")
            }
            mongoSink("payments", "mytopic", mongodb)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")
}

fun main() = stream {
    // create a config, named 'mypostgres', pointing to host "postgres" at port 5432, with username "postgres", password: "mysecretpassword" and use database "dvdrental"
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")

    // create a mongodb config, named 'mymongo' pointing to uri: 'mongodb://mongo' using database: "mydatabase"
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")

    // create a source, using schema "public" and table "film" and use the postgres
    postgresSource("public", "payment", pgConfig) {
        // For each key, message, and secondary message, log the key
        each {
            key, _, _ -> logger.info("Key: $key")
        }
        // ... add more transformers
        mongoSink("justfilm", "justfilm", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")

fun main2() = stream {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        joinRemote({ msg -> msg["language_id"].toString() }) { postgresSource("public", "language", pgConfig) {} }
        set { _, film, language ->
            film["language"] = language["name"]; film
        }
        mongoSink("filmwithlanguage", "filmwithlanguage", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")

fun mainold() {
    stream("mygeneration") {
        val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
        postgresSource("public", "actor", pgConfig) {
            set { _, msg, _ -> msg["last_update"] = null; msg }
            filter { _, msg -> (msg["actor_id"] as Int) < 10 }
            mongoSink("mycollection", "sometopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")
}
