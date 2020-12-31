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

import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

fun main() {
    stream {
        val postgresConfig = postgresSourceConfig("mypostgres", "postgres", 5432, "postgres", "mysecretpassword", "dvdrental", "public")
        val mongoConfig = mongoConfig("mongosink", "mongodb://mongo", "@mongodump")
        postgresSource("address", postgresConfig) {
            joinRemote({ msg -> "${msg["city_id"]}" }, false) {
                postgresSource("city", postgresConfig) {
                    joinRemote({ msg -> "${msg["country_id"]}" }, false) {
                        postgresSource("country", postgresConfig) {}
                    }
                    set { _, msg, state ->
                        msg.set("country", state)
                    }
                }
            }
            set { _, msg, state ->
                msg.set("city", state)
            }
            sink("@address")
        }
        postgresSource("customer", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("@address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            join(optional = true) {
                source("@customertotals") {}
            }
            set { _, customer, totals ->
                customer["total"] = totals["total"]; customer
            }
            mongoSink("customer", "customer", mongoConfig)
        }
        postgresSource("store", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("@address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            mongoSink("store", "store", mongoConfig)
        }
        postgresSource("staff", postgresConfig) {
            joinRemote({ m -> "${m["address_id"]}" }, false) {
                source("@address") {}
            }
            set { _, msg, state ->
                msg.set("address", state)
            }
            mongoSink("staff", "staff", mongoConfig)
        }
        postgresSource("payment", postgresConfig) {
            scan(
                { msg -> msg["customer_id"].toString() },
                { msg -> empty().set("total", 0.0).set("customer_id", msg["customer_id"]) },
                {
                    set { _, msg, state ->
                        state["total"] = state["total"] as Double + msg["amount"] as Double
                        state["customer_id"] = msg["customer_id"]!!
                        state
                    }
                },
                {
                    set { _, msg, state -> state["total"] = state["total"] as Double - msg["amount"] as Double; state }
                }
            )
            set { _, customer, totals ->
                customer["total"] = totals["total"]; customer
            }
            sink("@customertotals")
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092")
}
