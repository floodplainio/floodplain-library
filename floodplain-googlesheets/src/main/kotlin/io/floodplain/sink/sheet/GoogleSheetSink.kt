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

import io.floodplain.kotlindsl.Config
import io.floodplain.kotlindsl.FloodplainSink
import io.floodplain.kotlindsl.InputReceiver
import io.floodplain.kotlindsl.PartialStream
import io.floodplain.kotlindsl.SourceTopic
import io.floodplain.kotlindsl.Stream
import io.floodplain.kotlindsl.Transformer
import io.floodplain.kotlindsl.floodplainSinkFromTask
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.streams.api.ProcessorName
import io.floodplain.streams.api.Topic
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

fun PartialStream.googleSheetsSink(config: GoogleSheetConfiguration) {
    var sheetConnectorClass = SheetSinkConnector::class.java.name
    logger.info("Sheet connector: $sheetConnectorClass")
    val configMap: Map<String, String> = mapOf(Pair("connector.class", sheetConnectorClass))
    val sink = SinkTransformer(Optional.of(ProcessorName.from(config.name)), config.topic, false, Optional.empty(), false)
    addTransformer(Transformer(sink))
}

private class GoogleSheetSink(config: GoogleSheetConfiguration, spreadsheetId: String, columns: List<String>)
fun Stream.googleSheetConfig(topic: String, name: String, spreadsheetId: String, columns: List<String>): GoogleSheetConfiguration {
    val googleSheetConfiguration = GoogleSheetConfiguration(name, Topic.from(topic), spreadsheetId, columns)
    this.addSinkConfiguration(googleSheetConfiguration)
    return googleSheetConfiguration
}

class GoogleSheetConfiguration(val name: String, val topic: Topic, val spreadsheetId: String, val columns: List<String>) :
    Config {
    override fun materializeConnectorConfig(): Pair<String, Map<String, String>> {
        val additional = mutableMapOf<String, String>()

        val settings = mutableMapOf("connector.class" to SheetSinkConnector::class.java.name,
            "value.converter.schemas.enable" to "false",
            "key.converter.schemas.enable" to "false",
            "value.converter" to "org.apache.kafka.connect.json.JsonConverter",
            "key.converter" to "org.apache.kafka.connect.json.JsonConverter",
            "delete.on.null.values" to "true",
            SheetSinkTask.SPREADSHEETID to spreadsheetId,
            SheetSinkTask.COLUMNS to columns.joinToString(",")
        )
        settings.putAll(additional)
        settings.forEach { (key, value) ->
            logger.info { "Setting: $key value: $value" }
        }
        return name to settings.toMap()
    }
    override fun sourceElements(): List<SourceTopic> {
        return emptyList<SourceTopic>()
    }

    override suspend fun connectSource(inputReceiver: InputReceiver) {
    }

    override fun sinkElements(): Map<Topic, FloodplainSink> {
        val (_, settings) = materializeConnectorConfig()
        val connector = SheetSinkConnector()
        connector.start(settings)

        val task = connector.taskClass().getDeclaredConstructor().newInstance() as SheetSinkTask
        task.start(settings)
        val sink = floodplainSinkFromTask(task)
        return mapOf(topic to sink)
    }
}
