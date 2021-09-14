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
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.BinaryNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.NumericNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.Source
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.stream
import io.floodplain.reactive.source.topology.CustomTopicSource
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.replication.factory.ReplicationFactory
import org.apache.kafka.common.serialization.Serdes
import org.hl7.fhir.instance.model.api.IBaseResource
import org.hl7.fhir.r4.model.Patient
import java.io.ByteArrayInputStream
import java.util.Base64

private val objectMapper = ObjectMapper()
val fhirParser: IParser = FhirContext.forR4().newJsonParser()

@Suppress("UNCHECKED_CAST")
fun <T : IBaseResource> parseFhirToMsg(parser: IParser, data: ByteArray, transform: (T) -> IMessage): IMessage {
    val resource = parser.parseResource(ByteArrayInputStream(data))
    // TODO make sure to create a clear error message if classcast issues arise
    return transform(resource as T)
}

fun PartialStream.fhirGeneric(topic: String, init: Source.() -> Unit = {}): Source {
    return fhirExistingSource(topic, ::genericResource, this.rootTopology, init)
}

fun Stream.fhirGeneric(topic: String, init: Source.() -> Unit = {}) {
    addSource(fhirExistingSource(topic, ::genericResource, this, init))
}

fun <T : IBaseResource> PartialStream.fhirSource(
    topic: String,
    transform: (T) -> IMessage,
    init: Source.() -> Unit = {}
): Source {
    return fhirSource<T>(topic, transform, init)
}

fun <T : IBaseResource> Stream.fhirSource(topic: String, transform: (T) -> IMessage, init: Source.() -> Unit = {}) {
    addSource(fhirExistingSource(topic, transform, this.rootTopology, init))
}

private fun <T : IBaseResource> fhirExistingSource(
    topic: String,
    transform: (T) -> IMessage,
    rootTopology: Stream,
    init: Source.() -> Unit = {}
): Source {
    val context = FhirContext.forR4()
    val parser: IParser = context.newJsonParser()
    // TODO re-use these?
    val parseInput = { data: ByteArray? ->
        if(data!=null) {
            val transformedMessage = parseFhirToMsg<T>(parser, data, transform)
            ReplicationFactory.standardMessage(transformedMessage.toImmutable())
        } else {
            ReplicationFactory.empty().withOperation(ReplicationMessage.Operation.DELETE)
        }
    }

    val sourceElement = CustomTopicSource(
        topic,
        { data -> Serdes.String().deserializer().deserialize(topic, data) },
        { data -> parseInput(data) }
    )
    val source = Source(rootTopology, sourceElement, rootTopology.topologyContext)
    source.init()
    return source
}

fun genericResource(resource: IBaseResource): IMessage {
    val jsonNode = objectMapper.readTree(fhirParser.encodeResourceToString(resource)) as ObjectNode
    return parseMessage(jsonNode)
}

private fun parseMessage(objectMessage: ObjectNode): IMessage {
    val message = empty()
    objectMessage.fields().forEach { (field, value) ->
        parseJSONObject(message, value, field)
    }
    return message
}

private fun parseJSONObject(current: IMessage, json: JsonNode, field: String) {
    // TODO unsure if I've got all cases covered
    when (json.nodeType) {
        JsonNodeType.BINARY -> {
            // Use base64, TODO create native type
            val binary = (json as BinaryNode).binaryValue()
            current[field] = Base64.getEncoder().encode(binary)
        }

        null,JsonNodeType.NULL,JsonNodeType.MISSING,JsonNodeType.POJO -> {
            // no-op
        }
        JsonNodeType.NUMBER -> {
            if (json.isFloat) {
                current[field] = (json as NumericNode).floatValue()
            }
            if (json.isLong) {
                current[field] = (json as NumericNode).longValue()
            }
            if (json.isInt) {
                current[field] = (json as NumericNode).intValue()
            }
        }
        JsonNodeType.STRING -> {
            current[field] = (json as TextNode).textValue()
        }
        JsonNodeType.BOOLEAN -> {
            current[field] = (json as BooleanNode).booleanValue()
        }
        JsonNodeType.ARRAY -> {
            val array = json as ArrayNode
            if (array.isEmpty) {
                return
            }
            val first = array[0]
            if (first.isObject) {
                current[field] = array.map { element ->
                    parseMessage(element as ObjectNode)
                }.toList()
            } else if (first.isTextual) {
                current[field] = array.map { element ->
                    (element as TextNode).textValue()
                }.joinToString(",")
            }
        }
        JsonNodeType.OBJECT -> {
            current[field] = parseMessage(json as ObjectNode)
        }
    }
}

fun main() {

    fun patientToMessage(patient: Patient): IMessage {
        val message = empty()
        message["name"] = patient.name
        message["birthdate"] = patient.birthDate
        return message
    }

    fun patientGeneric(patient: Patient) {
        val context = FhirContext.forR4()
        val parser: IParser = context.newJsonParser()
        val stringed = parser.encodeResourceToString(patient)
        val objectMapper = ObjectMapper()
        val json = objectMapper.readTree(stringed)
    }
    stream {
        fhirSource<Patient>("sometopic", ::patientToMessage) {
        }
    }
}
