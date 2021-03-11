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
package io.floodplain.runtime
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.SystemExitException
import com.xenomachina.argparser.default
import io.floodplain.kotlindsl.LocalContext
import io.floodplain.kotlindsl.Stream
import io.floodplain.streams.api.TopologyContext
import java.io.OutputStreamWriter
import java.net.URL
import java.nio.charset.StandardCharsets

class LocalArgs(parser: ArgParser) {
    val force by parser.flagging(
        "-f",
        "--force",
        help = "force redeploy of connect modules. Not for local executions."
    )

    val id by parser.storing(
        "-i",
        "--id",
        help = "run as this application id. Subsequent runs with the same id will re-use storage. " +
            "Will be random if unspecified. For local runs this will only affect disk storage, for Kafka " +
            "based runs topics / consumer as well"
    )
        .default<String?>(null)

    val bufferTime by parser.storing(
        "-b",
        "--bufferTime",
        help = "Hints max buffering time. Longer increases latency, but might improve thoughput, esp. for " +
            "high-latency sinks. Only for local runs."
    ) { toInt() }
        .default(1000)

    val kafka by parser.storing(
        "-k",
        "--kafka",
        help =
            """Point to a kafka cluster e.g. localhost:9092 if none is given, floodplain will run in kafkaless mode. 
|                 If a kafka cluster is supplied, you probably need a --connect as well""".trimMargin()
    )
        .default<String?>(null)

    val connect by parser.storing(
        "-c",
        "--connect",
        help =
            """point to a connect instance e.g. http://localhost:8083
        |Make sure that this connect instance contains all required connector code
        """.trimMargin()
    )
        .default<String?>(null)
}

suspend fun run(stream: Stream, arguments: Array<out String?>, localContext: (suspend LocalContext.(TopologyContext) -> Unit)?, remoteContext: (suspend (AutoCloseable, TopologyContext) -> Unit)?) {

    val parseInto = try {
        ArgParser(arguments.filterNotNull().toTypedArray()).parseInto(::LocalArgs)
    } catch (e: SystemExitException) {
        val writer = OutputStreamWriter(if (e.returnCode == 0) System.out else System.err, StandardCharsets.UTF_8)
        e.printUserMessage(writer, System.getProperty("com.xenomachina.argparser.programName"), 80)
        writer.flush()
        return
    }
    parseInto.run {
        if (kafka != null) {
            if (connect == null) {
                throw RuntimeException("When supplying kafka, supply connect too")
            }
            val streamsInstance = stream.renderAndSchedule(URL(connect), kafka!!, force)
            remoteContext?.invoke(streamsInstance, stream.topologyContext)
        } else {
            stream.renderAndExecute(id, bufferTime) {
                localContext?.invoke(this, this.topologyContext())
            }
        }
    }
}
