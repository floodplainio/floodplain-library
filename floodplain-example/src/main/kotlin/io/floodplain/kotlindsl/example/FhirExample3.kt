package io.floodplain.kotlindsl.example

import io.floodplain.fhir.fhirGeneric
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.debeziumSource
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinMulti
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.MongoConfig
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.toMongo
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun Stream.conditionsWithResponse(mongoConfig: MongoConfig) {
    fhirGeneric("$deployment-Condition") {
        // evidence[0]/detail[0]/id
        // This will fail when there is no evidence associated with a condition. Is that reasonable?
        joinRemote({msg-> msg.messageList("evidence")!![0].messageList("detail")!![0].string("id")},false) {
            fhirGeneric("$deployment-QuestionnaireResponse")
        }
        set { _, condition, questionnaire ->
            condition["questionnaire"] = questionnaire
            condition
        }
        // set { _, condition, patient ->
        //     condition["patient"] = patient
        //     condition
        // }
        sink("$deployment-$generation-ConditionWithResponse")
        toMongo("ConditionWithQuestionnaire","$deployment-$generation-ConditionWithQuestionnaire",mongoConfig)
    }
}

fun Stream.patientsWithConditions(mongoConfig: MongoConfig) {
    fhirGeneric("$deployment-Patient") {
        joinGrouped(optional = false) {
            source("$deployment-$generation-ConditionWithResponse") {
                group { condition->condition.string("subject/id") }
            }
        }
        set { _, patient, conditionList ->
            patient["Conditions"] = conditionList["list"]
            patient
        }
        toMongo("PatientWithConditions", "$deployment-$generation-PatientWithConditions", mongoConfig)
    }
}

fun Stream.appointments(mongoConfig: MongoConfig) {
    debeziumSource("quin-$deployment-appointmentservices.public.appointment_entity") {
        joinRemote("patient_details_id") {
            debeziumSource("quin-$deployment-appointmentservices.public.patient_details_entity")
        }
        set { k, appointment, patient ->
            logger.info("Merging key: $k video: $appointment with patient: $patient")
            appointment["Patient"] = patient
            appointment["start_time"] = appointment.optionalDateTime("start_time")?:""
            appointment["end_time"] = appointment.optionalDateTime("end_time")?:""
            appointment
        }
        toMongo("Appointments", "$deployment-$generation-Appointments", mongoConfig)
    }
}
fun Stream.videoCalls(mongoConfig: MongoConfig) {
    debeziumSource("quin-$deployment-videoservices.public.video_call_entity") {
        joinRemote("patient_details_id") {
            debeziumSource("quin-$deployment-videoservices.public.patient_details_entity") {
                filter { _, patient -> patient["user_id"] != null }
                joinMulti({message->message.string("user_id")}, { message-> message.string("login_identifier") }, true) {
                    debeziumSource("quin-$deployment-user.public.user_") {
                        filter { _, user ->
                            val loginId = user.optionalString("login_identifier") ?: "email|"
                            !loginId.startsWith("email|")
                        }
                    }
                }
                set { _, patient, users ->
                    val userList = users.messageList("list")
                    if (userList?.isNotEmpty() == true) {
                        patient.merge(userList[0])
                    }
                    patient
                }
                filter { _, patient -> patient["fhir_id"] != null }
            }
        }
        set { k, video, patient ->
            video["Patient"] = patient
            video
        }
        toMongo("VideoCalls", "$deployment-$generation-VideoCalls", mongoConfig)
    }
}
fun main() {
    stream("quin","local", "23") {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        conditionsWithResponse(mongoConfig)
        patientsWithConditions(mongoConfig)
        videoCalls(mongoConfig)
        appointments(mongoConfig)
    }.renderAndSchedule(URL("http://localhost:8083/connectors"), "localhost:9092", true)
}
