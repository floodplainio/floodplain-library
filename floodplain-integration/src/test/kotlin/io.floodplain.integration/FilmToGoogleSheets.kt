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
package io.floodplain.integration

import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.postgresSource
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.scan
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.sink.sheet.SheetSink
import io.floodplain.sink.sheet.SheetSinkTask
import io.floodplain.sink.sheet.googleSheetConfig
import io.floodplain.sink.sheet.googleSheetsSink
import io.floodplain.streams.api.Topic
import io.floodplain.test.InstantiatedContainer
import io.floodplain.test.useIntegraton
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Test
import java.math.BigDecimal
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private val logger = mu.KotlinLogging.logger {}

@kotlinx.coroutines.ExperimentalCoroutinesApi
class FilmToGoogleSheets {

    private val spreadsheetId = "1MTAn1d13M8ptb2MkBHOSNK1gbJOOW1sFQoSfqa1JbXU"

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)

    @After
    fun shutdown() {
        postgresContainer.close()
    }

    @Test
    fun testPostgresGoogleSheets() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        val sheetSink = SheetSink()
        // First, clear the spreadsheet
        val rangesToClear = listOf(
            "Sheet1!" +
                "A1:H1100"
        )
        val clearedOk = sheetSink.clear(spreadsheetId, rangesToClear)
        assertEquals(rangesToClear, clearedOk, "Clearing seems to have failed")
        stream {

            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val sheetConfig = googleSheetConfig("sheets")
            postgresSource("film", postgresConfig) {
                // Clear the last_update field, it makes no sense in a denormalized situation
                set { _, film, _ ->
                    film["last_update"] = null
                    film["_row"] = film.integer("film_id").toLong()
                    film["special_features"] = film.list("special_features").joinToString(",")
                    film
                }
                googleSheetsSink("outputtopic", spreadsheetId, listOf("title", "rating", "release_year", "rental_duration", "special_features", "description"), "B", 2, sheetConfig)
            }
        }.renderAndExecute {
            // delay(5000)
            val ll = this.sinksByTopic()[Topic.from("outputtopic", topologyContext())]?.first()
            val task = ll!!.taskObject() as SheetSinkTask // ?.config()?.sinkTask()!! as SheetSinkTask
            val coreSink = task.sheetSink
            // coreSink.
            logger.info("Outputs: ${outputs()}")
            delay(5000)
            flushSinks()
            val elapsed = measureTimeMillis {
                withTimeout(200000) {
                    repeat(1000) {
                        // load a range:
                        val range: List<List<Any>>? = coreSink.getRange(spreadsheetId, "B3")
                        if (range != null) {
                            val value = range.first().first()
                            if (value == "Academy Dinosaur") {
                                logger.info("Found cell")
                                return@withTimeout
                            }
                        }
                        delay(500)
                    }
                }
            }
            logger.info("Google sheet sink took: $elapsed millis")
            val value = coreSink.getRange(spreadsheetId, "B3").first()?.first()
            assertEquals(value, "Academy Dinosaur")
        }
    }

    @Test
    fun testReduceGoogleSheets() {
        if (!useIntegraton) {
            logger.info("Not performing integration tests, doesn't seem to work in circleci")
            return
        }
        val sheetSink = SheetSink()
        // First, clear the spreadsheet
        sheetSink.clear(spreadsheetId, listOf("A1:H1100"))
        stream {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                postgresContainer.host,
                postgresContainer.exposedPort,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val sheetConfig = googleSheetConfig("sheets")
            postgresSource("payment", postgresConfig) {
                scan(
                    { empty().set("total", BigDecimal(0)) },
                    {
                        set { _, msg, state ->
                            state["total"] = (state["total"] as BigDecimal).add(msg["amount"] as BigDecimal)
                            state
                        }
                    },
                    {
                        set { _, msg, state ->
                            state["total"] = (state["total"] as BigDecimal).subtract(msg["amount"] as BigDecimal)
                            state
                        }
                    }
                )
                set { _, total, _ ->
                    total["_row"] = 2L
                    total
                }
                googleSheetsSink("outputtopic", spreadsheetId, listOf("total"), "B", 2, sheetConfig)
            }
        }.renderAndExecute {
            // delay(5000)
            val ll = this.sinksByTopic()[Topic.from("outputtopic", topologyContext())]?.first()
            val task = ll!!.taskObject() as SheetSinkTask // ?.config()?.sinkTask()!! as SheetSinkTask
            val coreSink = task.getSheetSink()
            // coreSink.
            logger.info("Outputs: ${outputs()}")
            delay(5000)
            flushSinks()
            val elapsed = measureTimeMillis {
                withTimeout(200000) {
                    repeat(1000) {
                        // load a range:
                        val range: List<List<Any>>? = coreSink.getRange(spreadsheetId, "B4")
                        if (range != null) {
                            val value = range.first().first() as String
                            val parsed = value.toDouble()
                            logger.info("Value: $parsed")
                            if (parsed > 61312) {
                                logger.info("Found cell")
                                return@withTimeout
                            }
                        }
                        delay(500)
                    }
                }
            }
            logger.info("Google sheet sink took: $elapsed millis")
            val value = coreSink.getRange(spreadsheetId, "B4").first()?.first() as String
            val parsed = value.toDouble()
            logger.info("Value: $parsed")
            assertTrue(parsed > 61312)
        }
    }
}
