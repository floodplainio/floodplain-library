package io.floodplain.kotlindsl.example

import fhirGeneric
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "8"
    val deployment = "local"
    stream(deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        fhirGeneric("local-Condition") {
            joinRemote({msg->msg.string("subject/id")},false) {
                fhirGeneric("local-Patient") {}
            }
            set { _, condition, patient ->
                condition["patient"] = patient
                condition
            }
            mongoSink("Condition", "@conditiontopic", mongoConfig)
        }
        fhirGeneric("local-Patient") {
            joinGrouped {
                fhirGeneric("local-Condition") {
                    group { condition->condition.string("subject/id") }
                }
            }
            set { _, patient, conditionList ->
                patient["Conditions"] = conditionList["list"]
                patient
            }
            mongoSink("Patient", "@patienttopic", mongoConfig)
        }
        fhirGeneric("local-Organization") {
            each { _, msg, _ ->
                logger.info("Organization name: ${msg["name"]}")
            }
            mongoSink("Org1","@sinktopic",mongoConfig)
        }
        // debeziumSource("quin-dev-videoservices.public.video_call_entity") {
        //     joinRemote("patient_details_id") {
        //         debeziumSource("quin-dev-videoservices.public.patient_details_entity") {}
        //     }
        //     set { _, call, patient ->
        //          call["patient"] = patient
        //          call
        //     }
        //     mongoSink("VideoCallEntity","@video_call_entity",mongoConfig)
        // }
        // debeziumSource("quin-dev-videoservices.public.patient_details_entity") {
        //     joinGrouped {
        //         debeziumSource("quin-dev-videoservices.public.video_call_entity") {
        //             group { it.string("patient_details_id") }
        //         }
        //     }
        //     set { _, patient, calls ->
        //         patient["calls"] = calls["list"]
        //         patient
        //     }
        //     mongoSink("CallsPerPatient","@video_patient_entity",mongoConfig)
        // }

    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092", true)
}
