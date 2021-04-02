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
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.joinAttributesQualified
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sinkQualified
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.localMongoConfig
import io.floodplain.mongodb.toMongo
import io.floodplain.replication.api.ReplicationMessage.KEYSEPARATOR
import java.time.Duration

fun main() {
    stream("KNBSB", "develop", "generation13") {
        val mongoConfig = localMongoConfig("$tenant-$deployment-$generation-mongosink", "mongodb://fp1", "$tenant-$deployment-$generation-mongodump")
        from("$tenant-$deployment-sportlinkkernel-CLASS") {
            set { _, msg, _ ->
                msg.clearAll("updateby", "lastupdate")
            }
            joinAttributesQualified(
                "$tenant-$deployment-sportlinkkernel-CLASSATTRIBUTE",
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
            sinkQualified("$tenant-$deployment-$generation-class")
            // mongoSink("class", "@class", mongoConfig)
        }
        from("$tenant-$deployment-sportlinkkernel-COMPETITIONTYPE") {
            set { _, msg, _ ->
                msg.clearAll("updateby", "lastupdate")
            }
            joinAttributesQualified(
                "$tenant-$deployment-sportlinkkernel-COMPETITIONTYPEATTRIBUTE",
                "attribname",
                "attribvalue",
                "competitiontypeid"
            )
            set { _, msg, attributes ->
                msg.merge(attributes)
            }
            joinRemote({ msg -> msg["ageclasscode"] as String }, false) {
                from("$tenant-$deployment-sportlinkkernel-AGECLASS")
            }
            set { _, competitiontype, ageclass ->
                competitiontype["ageclassname"] = ageclass["ageclassname"]
                competitiontype
            }
            buffer(Duration.ofSeconds(5))
            sinkQualified("$tenant-$deployment-$generation-competitiontype")
            toMongo("competitiontype", "$tenant-$deployment-$generation-competitiontypemongo", mongoConfig)
        }
        from("$tenant-$deployment-sportlinkkernel-CLASSSEASON") {
            joinRemote({ msg -> msg["classid"].toString() + KEYSEPARATOR + msg["competitiontypeid"].toString() + KEYSEPARATOR + msg["organizingdistrictid"].toString() }) {
                from("$tenant-$deployment-$generation-class")
            }
            set { _, classSeason, clazz ->
                classSeason["Class"] = clazz
                classSeason
            }
            joinRemote("competitiontypeid") {
                from("$tenant-$deployment-$generation-competitiontype")
            }
            set { _, classSeason, competitionType ->
                classSeason["CompetitionType"] = competitionType
                classSeason
            }
            sinkQualified("$tenant-$deployment-$generation-classseason")
            toMongo("classseasonwip", "$tenant-$deployment-$generation-classseasonmongo", mongoConfig)
        }
        from("$tenant-$deployment-sportlinkkernel-POOL") {
            joinRemote("classid", "competitiontypeid", "organizingdistrictid", "seasonid") {
                from("$tenant-$deployment-$generation-classseason")
            }
            set { _, pool, classseason ->
                pool["ClassSeason"] = classseason
                pool
            }
            joinRemote("schemaid", "periodid", optional = true) {
                from("$tenant-$deployment-sportlinkkernel-SCHEMAPERIOD")
            }
            set { _, pool, schema ->
                pool["SchemaPeriod"] = schema
                pool
            }
            sinkQualified("$tenant-$deployment-$generation-pool")
            toMongo("pool", "$tenant-$deployment-$generation-poolmongo", mongoConfig)
        }
        from("$tenant-$deployment-sportlinkkernel-TEAMCOMPETITIONTYPESEASON") {
            joinAttributesQualified(
                "$tenant-$deployment-sportlinkkernel-TEAMCOMPTPSEASONATTRIB",
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
                from("$tenant-$deployment-$generation-pool")
            }
            set { _, tcts, pool ->
                tcts["Pool"] = pool
                tcts
            }
            buffer(Duration.ofSeconds(5))
            sinkQualified("$tenant-$deployment-$generation-teamcompetitiontype")
            toMongo("teamcompetitiontype", "$tenant-$deployment-$generation-teamcompetitiontypemongo", mongoConfig)
        }
    }.renderAndSchedule(null, "10.8.0.7:9092")
}

// <store topic="sportlinkkernel-ZIPCODEPOSITION">
// <remove name="updateby"/>
// <remove name="lastupdate"/>
// <createcoordinate from="longitude,latitude" to="point"/>
// </store>
