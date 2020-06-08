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

import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.mongoConfigOld
import io.floodplain.kotlindsl.mongoSinkOld
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {
    joinFilms("generation1")
}

fun joinFilms(generation: String) {
    stream(generation) {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental")
        val mongoConfig = mongoConfigOld("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("public", "film", postgresConfig) {
            joinGrouped {
                postgresSource("public", "film_category", postgresConfig) {
                    joinRemote({ msg -> "${msg["category_id"]}" }, true) {
                        postgresSource("public", "category", postgresConfig) {}
                    }
                    set { _, msg, state ->
                        msg["category"] = state["name"] ?: "unknown"
                        msg
                    }
                    group { msg -> "${msg["film_id"]}" }
                }
            }
            set { _, msg, state ->
                msg["categories"] = state["list"] ?: empty()
                msg
            }
            mongoSinkOld("filmwithcategories", "filmwithcat", mongoConfig)
        }
    }.renderAndStart(URL("http://localhost:8083/connectors"), "localhost:9092")
    logger.info { "done!" }
}
