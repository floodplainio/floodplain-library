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
import io.floodplain.kotlindsl.joinAttributes
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.kotlindsl.topology
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.floodplain.replication.api.ReplicationMessage.KEYSEPARATOR
import java.net.URL
import java.time.Duration

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "generation11"
    val deployment = "develop"
    val tenant = "KNBSB"

    topology(tenant, deployment, generation) {
        source("bla")
    }
    stream(tenant, deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        source("sportlinkkernel-CLASS") {
            set { _, msg, _ ->
                msg.clearAll(listOf("updateby", "lastupdate"))
            }
            joinAttributes(
                "sportlinkkernel-CLASSATTRIBUTE",
                "attribname",
                "attribvalue",
                "classid",
                "competitiontypeid",
                "organizingdistrictid"
            )
            set { _, msg, attributes ->
                msg.merge(attributes)
                msg
            }
            sink("@class", false)
            // mongoSink("class", "@class", mongoConfig)
        }
        source("sportlinkkernel-COMPETITIONTYPE") {
            set { _, msg, _ ->
                msg.clearAll(listOf("updateby", "lastupdate"))
            }
            joinAttributes(
                "sportlinkkernel-COMPETITIONTYPEATTRIBUTE",
                "attribname",
                "attribvalue",
                "competitiontypeid"
            )
            set { _, msg, attributes ->
                msg.merge(attributes)
            }
            joinRemote({ msg -> msg["ageclasscode"] as String }, false) {
                source("sportlinkkernel-AGECLASS") {}
            }
            set { _, competitiontype, ageclass ->
                competitiontype["ageclassname"] = ageclass["ageclassname"]
                competitiontype
            }
            buffer(Duration.ofSeconds(5))
            sink("@competitiontype")
            mongoSink("competitiontype", "@competitiontypemongo", mongoConfig)
        }
        source("sportlinkkernel-CLASSSEASON") {
            joinRemote({ msg -> msg["classid"].toString() + KEYSEPARATOR + msg["competitiontypeid"].toString() + KEYSEPARATOR + msg["organizingdistrictid"].toString() }) {
                source("@class")
            }
            set { _, classSeason, clazz ->
                classSeason["Class"] = clazz
                classSeason
            }
            joinRemote("competitiontypeid") {
                source("@competitiontype")
            }
            set { _, classSeason, competitionType ->
                classSeason["CompetitionType"] = competitionType
                classSeason
            }
            sink("@classseason")
            mongoSink("classseasonwip", "@classseasonmongo", mongoConfig)
        }
        source("sportlinkkernel-POOL") {
            joinRemote("classid", "competitiontypeid", "organizingdistrictid", "seasonid") {
                source("@classseason")
            }
            set { _, pool, classseason ->
                pool["ClassSeason"] = classseason
                pool
            }
            joinRemote("schemaid", "periodid", optional = true) {
                source("sportlinkkernel-SCHEMAPERIOD")
            }
            set { _, pool, schema ->
                pool["SchemaPeriod"] = schema
                pool
            }
            sink("@pool")
            mongoSink("pool", "@poolmongo", mongoConfig)
        }
        source("sportlinkkernel-TEAMCOMPETITIONTYPESEASON") {
            joinAttributes(
                "sportlinkkernel-TEAMCOMPTPSEASONATTRIB",
                "attribname",
                "attribvalue",
                "competitiontypeid",
                "seasonid",
                "teamid"
            )
            set { _, msg, attributes ->
                msg.merge(attributes)
            }
            // TODO missing bypass: bypass="notnull:poolid"
            joinRemote("poolid") {
                source("@pool")
            }
            set { _, tcts, pool ->
                tcts["Pool"] = pool
                tcts
            }
            buffer(Duration.ofSeconds(5))
            sink("@teamcompetitiontype")
            mongoSink("teamcompetitiontype", "@teamcompetitiontypemongo", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "10.8.0.7:9092")
    logger.info { "done!" }
}

// <store topic="sportlinkkernel-ZIPCODEPOSITION">
// <remove name="updateby"/>
// <remove name="lastupdate"/>
// <createcoordinate from="longitude,latitude" to="point"/>
// </store>
