package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinAttributes
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

fun main() {

    val generation = "generation28"
    val deployment = "develop"
    val tenant = "KNBSB"

    stream(tenant, deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        source("sportlinkkernel-MATCH") {
            set { _, match, _ ->
                match.clearAll(listOf("updateby", "lastupdate"))
                match["publicmatchid"] = createPublicId("M", 776533757, 403789397, 2030118221, match.integer("matchid"))
                // <publicid field="matchid" to="publicmatchid" prefix="M" prime="776533757" modInverse="403789397" random="2030118221"/>
                match
            }
            joinAttributes("sportlinkkernel-MATCHATTRIBUTE", "attribname", "attribvalue", "matchid")
            set { _, msg, attributes ->
                msg.merge(attributes)
            }
            joinGrouped {
                source("sportlinkkernel-CALENDARDAY") {
                    // match["matchdatetime"] = combineDateTime(match.date("calendardate"),match.date("startdatetime"))
                    filter { key, msg ->
                        filterValidCalendarActivityId(key, msg)
                    }
                    set { _, calendarday, _ ->
                        calendarday.clearAll(listOf("updateby", "lastupdate"))
                        calendarday
                    }
                    group { msg -> msg.integer("activityid").toString() }
                }
            }
            set { _, match, calendarday ->
                match["CalendarDay"] = calendarday["list"] ?: emptyList<IMessage>()
                match
            }
            mongoSink("match", "@sometopic", mongoConfig)
        }
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "10.8.0.7:9092")
}
