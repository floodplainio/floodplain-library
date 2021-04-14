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
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.stream
import io.floodplain.sink.sheet.GoogleSheetConfiguration
import io.floodplain.sink.sheet.googleSheetConfig
import io.floodplain.sink.sheet.googleSheetsSink
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import kotlinx.coroutines.delay
import org.junit.Test
import java.util.Optional

private val logger = mu.KotlinLogging.logger {}

class GoogleSheetTest {

    private val spreadsheetId = "1MTAn1d13M8ptb2MkBHOSNK1gbJOOW1sFQoSfqa1JbXU"
    // var spreadsheetId = "1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4"

    @Test
    fun testGoogleSheet() {
        // sanity check
        GoogleSheetConfiguration(TopologyContext.context(Optional.empty(), "1"), TopologyConstructor(), "connectorName")
    }

    @Test
    fun testSheetWithTopology() {
        stream {
            val config = googleSheetConfig("somename")
            from("topic") {
                each { _, msg, _ -> logger.info("MSG: $msg") }
                set { _, msg, _ ->
                    msg["_row"] = msg.integer("id").toLong()
                    msg
                }
                googleSheetsSink(
                    "outputtopic",
                    spreadsheetId,
                    listOf("column1", "column2"),
                    "A",
                    1,
                    config
                )
            }
        }.renderAndExecute {
            input("topic", "k1", empty().set("column1", "kol1").set("column2", "otherkol1").set("id", 1))
            input("topic", "k2", empty().set("column1", "kol2").set("column2", "otherkol2").set("id", 2))
            // delay(1000)
            delay(1000)
            input("topic", "k3", empty().set("column1", "kol3").set("column2", "otherkol3").set("id", 3))
            delay(1000)
            // TODO improve testing
        }
        // Thread.sleep(200000)
    }
}
