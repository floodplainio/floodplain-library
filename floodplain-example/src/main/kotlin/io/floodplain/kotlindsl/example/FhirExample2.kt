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
package io.floodplain.fhir.example

import io.floodplain.fhir.fhirGeneric
import io.floodplain.kotlindsl.debeziumSource
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinMulti
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.net.URL

private val logger = mu.KotlinLogging.logger {}

fun main() {
    stream("local", "20") {
        val mongoConfig = mongoConfig("@mongosink", "mongodb://mongo", "@mongodump")
        fhirGeneric("$deployment-Condition") {
            // evidence[0]/detail[0]/id
            joinRemote({ msg -> msg.messageList("evidence")!![0].messageList("detail")!![0].string("id") }, false) {
                fhirGeneric("$deployment-QuestionnaireResponse")
            }
            set { _, condition, questionnaire ->
                condition["questionnaire"] = questionnaire
                condition
            }
            joinRemote({ msg -> msg.string("subject/id") }, false) {
                fhirGeneric("$deployment-Patient")
            }
            set { _, condition, patient ->
                condition["patient"] = patient
                condition
            }
            sink("@ConditionWithResponse")
        }
        fhirGeneric("$deployment-Patient") {
            joinGrouped {
                fhirGeneric("$deployment-$generation-ConditionWithResponse") {
                    group { condition -> condition.string("subject/id") }
                }
            }
            set { _, patient, conditionList ->
                patient["Conditions"] = conditionList["list"]
                patient
            }
            sink("@PatientWithConditions")
        }
        fhirGeneric("$deployment-Organization") {
            each { _, msg, _ ->
                logger.info("Organization name: ${msg["name"]}")
            }
            mongoSink("Org1", "@sinktopic", mongoConfig)
        }
        debeziumSource("quin-dev-videoservices.public.video_call_entity") {
            joinRemote("patient_details_id") {
                debeziumSource("quin-dev-videoservices.public.patient_details_entity") {
                    mongoSink("intermediateSink", "intermTopic", mongoConfig)
                    filter { _, patient -> patient["user_id"] != null }
                    mongoSink("intermediateSink2", "intermTopic2", mongoConfig)
                    each { _, patientDetails, _ -> logger.info("Patient details detected: $patientDetails") }
                    joinMulti({ patient -> patient.string("user_id") }, { user -> user.string("login_identifier") }, true) {
                        debeziumSource("quin-dev-user.public.user_") {
                            filter { _, user ->
                                val loginId = user.optionalString("login_identifier") ?: "email|"
                                !loginId.startsWith("email|")
                            }
                        }
                    }
                    set { _, patient, users ->
                        val userList = users.messageList("list")
                        if (userList?.isNotEmpty() == true) {
                            patient["user"] = userList[0]
                        }
                        patient
                    }
                }
            }
            set { _, call, patient ->
                call["patient"] = patient
                call
            }
            mongoSink("VideoCall", "@video_call_entity", mongoConfig)
        }

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
