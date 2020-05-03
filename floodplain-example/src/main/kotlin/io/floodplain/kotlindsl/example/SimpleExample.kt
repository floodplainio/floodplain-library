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
import io.floodplain.kotlindsl.mongoConfig
import io.floodplain.kotlindsl.mongoSink
import io.floodplain.kotlindsl.pipe
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import java.net.URL
import java.util.UUID

private val logger = mu.KotlinLogging.logger {}

fun main() = pipe {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        each {
            key, message, sec -> logger.info("Key: $key")
        }
        mongoSink("justfilm", "justfilm", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())

fun main2() = pipe {
    val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
    val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
    postgresSource("public", "film", pgConfig) {
        joinRemote({ msg -> msg["language_id"].toString() }) { postgresSource("public", "language", pgConfig) {} }
        set { _, film, language ->
            film["language"] = language["name"]; film
        }
        mongoSink("filmwithlanguage", "filmwithlanguage", mongoConfig)
    }
}.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())

fun mainold() {
    pipe("mygeneration") {
        val pgConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfig("mymongo", "mongodb://mongo", "mydatabase")
        postgresSource("public", "actor", pgConfig) {
            set { _, msg, _ -> msg["last_update"] = null; msg }
            filter { _, msg -> (msg["actor_id"] as Int) < 10 }
            mongoSink("mycollection", "sometopic", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092", UUID.randomUUID().toString())
}
