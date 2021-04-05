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
package io.floodplain.kotlindsl

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.kafka.connect.storage.Converter
import java.io.IOException
import java.net.URL
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

private val logger = mu.KotlinLogging.logger {}
private val objectMapper = ObjectMapper()
const val DEFAULT_HTTP_TIMEOUT = 10L
val httpClient: HttpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .followRedirects(HttpClient.Redirect.NORMAL)
    .connectTimeout(Duration.ofSeconds(DEFAULT_HTTP_TIMEOUT))
    .build()

fun constructConnectorJson(
    topologyContext: TopologyContext,
    connectorName: String,
    parameters: Map<String, Any>
): String {
    val generatedName = topologyContext.topicName(connectorName)
    val node = objectMapper.createObjectNode()
    node.put("name", generatedName)
    val configNode = objectMapper.createObjectNode()
    node.set<JsonNode>("config", configNode)
    parameters.forEach { (k: String, v: Any) ->
        when (v) {
            is String -> {
                configNode.put(k, v as String?)
            }
            is Int -> {
                configNode.put(k, v as Int?)
            }
            is Long -> {
                configNode.put(k, v as Long?)
            }
            is Float -> {
                configNode.put(k, v as Float?)
            }
            is Double -> {
                configNode.put(k, v as Double?)
            }
            is Boolean -> {
                configNode.put(k, v as Boolean?)
            }
        }
    }
    // override name to match general name
    configNode.put("name", generatedName)
    configNode.put("database.server.name", generatedName)
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node)
}

fun startConstructor(
    connectorName: String,
    topologyContext: TopologyContext,
    connectURL: URL,
    jsonString: String,
    force: Boolean
) {
    val generatedName = topologyContext.topicName(connectorName)
    val current = existingConnectors(connectURL)
    if (current.contains(generatedName)) {
        if (force) {
            logger.warn("Force enabled, deleting old")
            deleteConnector(generatedName, connectURL)
        } else {
            logger.warn("Connector: {} already present, ignoring", generatedName)
            // TODO return from here?
        }
    }
    postToHttpJava11(connectURL, jsonString)
}

private fun existingConnectors(url: URL): List<String> {
    logger.info("Connecting to URL: $url")
    val response = httpClient.send(HttpRequest.newBuilder().uri(url.toURI()).build(), BodyHandlers.ofInputStream())
    val an = objectMapper.readTree(response.body()) as ArrayNode
    val result: MutableList<String> = ArrayList()
    an.forEach(Consumer { j: JsonNode -> result.add(j.asText()) })
    return Collections.unmodifiableList(result)
}

@Throws(IOException::class)
private fun deleteConnector(name: String, connectURL: URL) {
    val url = URL("$connectURL/$name")
    val request: HttpRequest = HttpRequest.newBuilder()
        .uri(url.toURI())
        .DELETE()
        .build()
    val response = httpClient.send(request, BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
        throw IOException("Error deleting connector: ${response.uri()}")
    }
}

private fun postToHttpJava11(url: URL, jsonString: String) {
    val request: HttpRequest = HttpRequest.newBuilder()
        .uri(url.toURI())
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonString))
        .build()
    val response: HttpResponse<String> = httpClient.send(request, BodyHandlers.ofString())
    if (response.statusCode() >= 400) {
        logger.error("Scheduling connector failed. Request: $jsonString")
        throw IOException(
            "Error calling connector: ${response.uri()} " +
                "code: ${response.statusCode()} body: ${response.body()}"
        )
    }
}

fun floodplainSinkFromTask(task: SinkTask, config: SinkConfig, keyConverter: Converter, valueConverter: Converter): FloodplainSink {
    return LocalConnectorSink(task, config,keyConverter,valueConverter)
}

fun instantiateSinkConfig(config: SinkConfig): MutableMap<Topic, MutableList<FloodplainSink>> {
    val result = mutableMapOf<Topic, MutableList<FloodplainSink>>()
    val materializedSinks = config.materializeConnectorConfig()
    materializedSinks.forEach { materializedSink ->
        val connectorInstance = Class.forName(materializedSink.settings["connector.class"]).getDeclaredConstructor().newInstance() as SinkConnector
        connectorInstance.start(materializedSink.settings)
        val task = connectorInstance.taskClass().getDeclaredConstructor().newInstance() as SinkTask
        task.start(materializedSink.settings)
        val keyConverter = Class.forName(materializedSink.settings["key.converter"]).getDeclaredConstructor().newInstance() as Converter
        val valueConverter = Class.forName(materializedSink.settings["value.converter"]).getDeclaredConstructor().newInstance() as Converter
        keyConverter.configure(settingsWithPrefix(materializedSink.settings,"key.converter."),true)
        valueConverter.configure(settingsWithPrefix(materializedSink.settings,"value.converter."),false)
        val localSink = floodplainSinkFromTask(task, config,keyConverter,valueConverter)
        materializedSink.topics.forEach { topic ->
            val list = result.computeIfAbsent(topic) { mutableListOf() }
            list.add(localSink)
        }
    }
    return result
    // return instance
    // return connectorInstance to instance
}

private fun settingsWithPrefix(settings: Map<String,Any>, prefix: String): Map<String,Any> {
    return settings.filter { (k,v) ->
        k.startsWith(prefix)
    }.map { (k,v) ->
        k.substring(prefix.length) to v
    }.toMap()
}

class LocalConnectorSink(private val task: SinkTask, val config: SinkConfig, val keyConverter: Converter,val valueConverter: Converter) : FloodplainSink {
    private val offsetCounter = AtomicLong(System.currentTimeMillis())
    override fun send(topic: Topic, elements: List<Pair<ByteArray?,ByteArray?>>) {
        logger.info("Inserting # of documents ${elements.size} for topic: $topic")
        val list = elements.map { (key, value) ->
            // logger.info("Key: ${String(key?: byteArrayOf())} \nValue: ${String(value?: byteArrayOf())}")
            SinkRecord(topic.qualifiedString(), 0, null,
                keyConverter.toConnectData(topic.qualifiedString(),key)?.value(), null,
                valueConverter.toConnectData(topic.qualifiedString(),value)?.value(),
                offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
    }

    override fun config(): SinkConfig {
        return config
    }

    override fun flush() {
        task.flush(emptyMap())
    }

    override fun close() {
        task.close(emptyList())
    }

    override fun taskObject(): Any? {
        return task
    }
}
