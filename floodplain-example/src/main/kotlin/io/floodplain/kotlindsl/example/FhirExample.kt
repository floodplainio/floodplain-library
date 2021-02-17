package io.floodplain.kotlindsl.example

import io.floodplain.fhir.fhirSource
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.debeziumSource
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinMulti
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.qualifiedTopic
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.MongoConfig
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.floodplain.mongodb.toMongo
import org.hl7.fhir.r4.model.Organization
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun Stream.video(mongoConfig: MongoConfig) {
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
    val source = object{}.javaClass.classLoader.getResourceAsStream("test_confluent.properties")
    stream("quin","test", "32") {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://root:OmXBJ3uGp3@datadriven-mongodb.quin-platform-dev", "@mongodump")
        video(mongoConfig)
    }.renderAndSchedule(URL("http://change-capture.quin-platform-test:8083/connectors"), source,true)
}

fun main2() {

    val source = object{}.javaClass.classLoader.getResourceAsStream("test_confluent.properties")
    stream("quin","local", "30") {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://root:OmXBJ3uGp3@datadriven-mongodb.quin-platform-dev", "@mongodump")
        fhirSource("$deployment-Organization", ::organizationParserSimple) {
            each { _, msg, _ ->
                logger.info("Organization name: ${msg["name"]}")
            }
            mongoSink("Org1","@sinktopic",mongoConfig)
        }
        // debeziumSource("$tenant-$deployment-videoservices.public.video_call_entity") {
        //     joinRemote("patient_details_id") {
        //         debeziumSource("$tenant-$deployment-videoservices.public.patient_details_entity") {}
        //     }
        //     set { _, call, patient ->
        //          call["patient"] = patient
        //          call
        //     }
        //     mongoSink("VideoCallEntity","@video_call_entity",mongoConfig)
        // }
        // debeziumSource("$tenant-$deployment-videoservices.public.patient_details_entity") {
        //     joinGrouped {
        //         debeziumSource("$tenant-$deployment-videoservices.public.video_call_entity") {
        //             group { it.string("patient_details_id") }
        //         }
        //     }
        //     set { _, patient, calls ->
        //         patient["calls"] = calls["list"]
        //         patient
        //     }
        //     mongoSink("CallsPerPatient","@video_patient_entity",mongoConfig)
        // }

    }.renderAndSchedule(URL("http://change-capture.quin-platform-test:8083/connectors"), source,true)
}

fun organizationParserSimple(org: Organization): IMessage {
        return empty().set("name",org.name).set("id",org.id)
}
