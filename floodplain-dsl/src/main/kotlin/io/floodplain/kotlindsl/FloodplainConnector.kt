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
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

private val logger = mu.KotlinLogging.logger {}
private val objectMapper = ObjectMapper()
val httpClient: HttpClient = HttpClient.newBuilder()
    .version(HttpClient.Version.HTTP_1_1)
    .followRedirects(HttpClient.Redirect.NORMAL)
    .connectTimeout(Duration.ofSeconds(10))
    .build()

fun constructConnectorJson(topologyContext: TopologyContext, connectorName: String, parameters: Map<String, Any>): String {
    val generatedName = topologyContext.topicName(connectorName)
    val node = objectMapper.createObjectNode()
    node.put("name", generatedName)
    val configNode = objectMapper.createObjectNode()
    node.set<JsonNode>("config", configNode)
    parameters.forEach { (k: String, v: Any) ->
        if (v is String) {
            configNode.put(k, v as String?)
        } else if (v is Int) {
            configNode.put(k, v as Int?)
        } else if (v is Long) {
            configNode.put(k, v as Long?)
        } else if (v is Float) {
            configNode.put(k, v as Float?)
        } else if (v is Double) {
            configNode.put(k, v as Double?)
        } else if (v is Boolean) {
            configNode.put(k, v as Boolean?)
        }
    }
    // override name to match general name
    configNode.put("name", generatedName)
    configNode.put("database.server.name", generatedName)
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node)
}

fun startConstructor(connectorName: String, topologyContext: TopologyContext, connectURL: URL, jsonString: String, force: Boolean) {
    val generatedName = topologyContext.topicName(connectorName)
    val current = existingConnectors(connectURL)
    if (current.contains(generatedName)) {
        if (force) {
            logger.warn("Force enabled, deleting old")
            deleteConnector(generatedName, connectURL)
        } else {
            logger.warn("Connector: {} already present, ignoring", generatedName)
        }
    }
    postToHttpJava11(connectURL, jsonString)
}

private fun existingConnectors(url: URL): List<String> {
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
        throw IOException("Error calling connector: ${response.uri()} code: ${response.statusCode()} body: ${response.body()}")
    }
}

fun floodplainSinkFromTask(task: SinkTask, config: Config): FloodplainSink {
    return LocalConnectorSink(task, config)
}

fun instantiateSinkConfig(topologyContext: TopologyContext, config: Config, connector: () -> SinkConnector): Map<Topic, MutableList<FloodplainSink>> {
    val result = mutableMapOf<Topic, MutableList<FloodplainSink>>()
    val materializedSinks = config.materializeConnectorConfig(topologyContext)
    materializedSinks.map { materializedSink ->
        val connectorInstance = connector()
        connector().start(materializedSink.settings)
        val task = connectorInstance.taskClass().getDeclaredConstructor().newInstance() as SinkTask
        task.start(materializedSink.settings)

        val localSink = floodplainSinkFromTask(task, config)
        materializedSink.topics.forEach { topic ->
            val list = result.computeIfAbsent(topic) { _ -> mutableListOf() }
            list.add(localSink)
        }
    }
    return result
}

private class LocalConnectorSink(private val task: SinkTask, val config: Config) : FloodplainSink {
    private val offsetCounter = AtomicLong(System.currentTimeMillis())
    override fun send(topic: Topic, elements: List<Pair<String, Map<String, Any>?>>, topologyContext: TopologyContext) {
        logger.info("Inserting # of documents ${elements.size} for topic: $topic")
        val list = elements.map { (key, value) ->
            SinkRecord(topic.qualifiedString(topologyContext), 0, null, key, null, value, offsetCounter.incrementAndGet())
        }.toList()
        task.put(list)
    }

    override fun config(): Config {
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
