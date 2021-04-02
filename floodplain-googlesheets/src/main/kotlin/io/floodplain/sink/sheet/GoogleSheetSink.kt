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
package io.floodplain.sink.sheet

import io.floodplain.kotlindsl.AbstractSinkConfig
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.MaterializedConfig
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.sink.sheet.SheetSinkTask.COLUMNS
import io.floodplain.sink.sheet.SheetSinkTask.SPREADSHEETID
import io.floodplain.sink.sheet.SheetSinkTask.STARTCOLUMN
import io.floodplain.sink.sheet.SheetSinkTask.STARTROW
import io.floodplain.sink.sheet.SheetSinkTask.TOPICS
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

fun PartialStream.googleSheetsSink(topicDefinition: String, googleSheetId: String, columns: List<String>, startColumn: String = "A", startRow: Int = 1, config: GoogleSheetConfiguration) {
    val sheetConnectorClass = SheetSinkConnector::class.java.name
    logger.info("Sheet connector: $sheetConnectorClass")
    val topic = Topic.from(topicDefinition, topologyContext)
    val sheetSink = GoogleSheetSink(topic, googleSheetId, columns, startColumn, startRow)
    config.addSink(sheetSink)
    val sink = SinkTransformer(Optional.of(config.name), topic, Optional.empty(), Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    addTransformer(Transformer(rootTopology, sink, topologyContext))
}

class GoogleSheetSink(val topic: Topic, val spreadsheetId: String, val columns: List<String>, val startColumn: String = "A", val startRow: Int = 1)
fun Stream.googleSheetConfig(name: String): GoogleSheetConfiguration {
    val googleSheetConfiguration = GoogleSheetConfiguration(topologyContext, topologyConstructor, name)
    this.addSinkConfiguration(googleSheetConfiguration)
    return googleSheetConfiguration
}

class GoogleSheetConfiguration(override val topologyContext: TopologyContext, override val topologyConstructor: TopologyConstructor, val name: String) :
    AbstractSinkConfig() {
    private var googleTask: SheetSinkTask? = null
    private val sheetSinks = mutableListOf<GoogleSheetSink>()
    private var instantiatedSinkElements: Map<Topic, MutableList<FloodplainSink>>? = null

    override fun materializeConnectorConfig(): List<MaterializedConfig> {
        sheetSinks.forEach { e -> topologyConstructor.addDesiredTopic(e.topic, Optional.of(1)) }
        return sheetSinks.map {
            val settings = mutableMapOf(
                "connector.class" to SheetSinkConnector::class.java.name,
                "value.converter.schemas.enable" to "false",
                "key.converter.schemas.enable" to "false",
                "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
                "delete.on.null.values" to "true"
            )
            // SheetSinkTask.SPREADSHEETID to spreadsheetId,
            // SheetSinkTask.COLUMNS to columns.joinToString(",")
            settings[SPREADSHEETID] = it.spreadsheetId
            settings[COLUMNS] = it.columns.joinToString(",")
            settings[TOPICS] = it.topic.qualifiedString()
            settings[STARTCOLUMN] = it.startColumn
            settings[STARTROW] = it.startRow.toString()
            // settings.put(SheetSinkTask.STARTCOLUMN,)
            MaterializedConfig(name, listOf(it.topic), settings)
        }
    }

    override fun sinkElements(): Map<Topic, MutableList<FloodplainSink>> {
        return instantiatedSinkElements ?: emptyMap()
    }

    override fun sinkTask(): Any? {
        return googleTask
    }

    fun addSink(sheetSink: GoogleSheetSink) {
        sheetSinks.add(sheetSink)
    }
}
