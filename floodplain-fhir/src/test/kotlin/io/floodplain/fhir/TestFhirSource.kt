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

package io.floodplain.fhir

import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.parser.IParser
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import org.hl7.fhir.r4.model.Patient
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import org.junit.jupiter.api.Assertions

class TestFhirSource {

    private fun patientToMessage(patient: Patient): IMessage {
        val message = empty()
        message["name"] = patient.name[0].family
        message["birthdate"] = patient.birthDate
        return message
    }
    @Test
    fun testParse() {
        val parser: IParser = FhirContext.forR4().newJsonParser()
        val data = this::class.java.classLoader.getResource("patient-example.json").readBytes()
        val msg = parseFhirToMsg(parser, data, ::patientToMessage)
        Assertions.assertEquals("Chalmers", msg["name"])
    }

    @Test
    fun testParseGenericFhirResource() {
        val parser: IParser = FhirContext.forR4().newJsonParser()
        val data = this::class.java.classLoader.getResource("patientexample.json").readBytes()
        val resource = parser.parseResource(ByteArrayInputStream(data))
        val message = genericResource(resource)
        println("message: $message")
    }
}
