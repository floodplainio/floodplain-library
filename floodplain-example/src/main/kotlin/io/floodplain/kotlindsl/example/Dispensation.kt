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

import io.floodplain.kotlindsl.buffer
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.fork
import io.floodplain.kotlindsl.joinAttributes
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.kotlindsl.streams
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.floodplain.replication.api.ReplicationMessage.KEYSEPARATOR
import java.net.URL
import java.time.Duration

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "generation12"
    val deployment = "develop"
    val tenant = "KNBSB"
    stream(tenant, deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        source("sportlinkkernel-DISPENSATION") {
            set { _, msg, _ ->
                msg.clearAll(listOf("updateby", "lastupdate"))
            }
            fork({
                filter { key,msg->
                    msg["targetorganizationid"] != null
                }
                sink("@organizationdispensation")
                mongoSink("organizationdispensation","@mongoorganizationdispensation",mongoConfig)
            }, {
                filter { key,msg->
                    msg["targetpersonid"] != null
                }
                sink("@persondispensation")
                mongoSink("persondispensation","@mongopersondispensation",mongoConfig)
            })
            sink("@class", false)
            // mongoSink("class", "@class", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "10.8.0.7:9092")
    logger.info { "done!" }
}

// <store topic="sportlinkkernel-ZIPCODEPOSITION">
// <remove name="updateby"/>
// <remove name="lastupdate"/>
// <createcoordinate from="longitude,latitude" to="point"/>
// </store>
