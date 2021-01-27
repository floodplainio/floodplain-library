package io.floodplain.kotlindsl.example

import io.floodplain.fhir.fhirSource
import io.floodplain.kotlindsl.debeziumSource
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import org.hl7.fhir.r4.model.Organization
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {

    val generation = "20"
    val deployment = "local"
    stream(deployment, generation) {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://root:OmXBJ3uGp3@datadriven-mongodb", "@mongodump")
        fhirSource("local-Organization", ::organizationParserSimple) {
            each { _, msg, _ ->
                logger.info("Organization name: ${msg["name"]}")
            }
            mongoSink("Org1","@sinktopic",mongoConfig)
        }
        debeziumSource("quin-dev-videoservices.public.video_call_entity") {
            joinRemote("patient_details_id") {
                debeziumSource("quin-dev-videoservices.public.patient_details_entity") {}
            }
            set { _, call, patient ->
                 call["patient"] = patient
                 call
            }
            mongoSink("VideoCallEntity","@video_call_entity",mongoConfig)
        }
        debeziumSource("quin-dev-videoservices.public.patient_details_entity") {
            joinGrouped {
                debeziumSource("quin-dev-videoservices.public.video_call_entity") {
                    group { it.string("patient_details_id") }
                }
            }
            set { _, patient, calls ->
                patient["calls"] = calls["list"]
                patient
            }
            mongoSink("CallsPerPatient","@video_patient_entity",mongoConfig)
        }

    }.renderAndSchedule(URL("http://change-capture:8083/connectors"), "kafka.quin-platform-dev.svc.cluster.local:9092",true)
}

fun organizationParserSimple(org: Organization): IMessage {
        return empty().set("name",org.name).set("id",org.id)
}
