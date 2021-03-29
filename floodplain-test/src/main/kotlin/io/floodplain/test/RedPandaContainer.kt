/**
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
package io.floodplain.test

import com.github.dockerjava.api.command.InspectContainerResponse
import org.testcontainers.containers.GenericContainer
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets

/**
 * Kotlin wrapper, to make testcontainers easier to use
 */
private const val KAFKA_PORT = 9092
private const val STARTER_SCRIPT = "/entry.sh"

class RedPandaContainer(image: String, port: Int, env: Map<String, String> = emptyMap()) {
    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(DockerImageName.parse(imageName)) {
        var port: Int? = null

        override fun doStart() {
            // withCommand(
            //     "sh",
            //     "-c",
            //     "while [ ! -f $STARTER_SCRIPT ]; do sleep 1; echo woopp; done; $STARTER_SCRIPT"
            // )
            withCreateContainerCmdModifier { cmd ->
                println("Bommmp: " + cmd)
                cmd.withEntrypoint("sh -c while [ ! -f $STARTER_SCRIPT ]; do sleep 1; done; $STARTER_SCRIPT")
            }
            println("Command parts: ${commandParts.asList()}")
            super.doStart()
        }
        override fun containerIsStarting(containerInfo: InspectContainerResponse, reused: Boolean) {
            super.containerIsStarting(containerInfo, reused)
            port = getMappedPort(KAFKA_PORT)

            if (reused) {
                return
            }
            val kafkaURL = "$host:$port"
            val startCommand =
                """
                #!/usr/bin/env bash
                echo woooop
                set -e
                supercronic -quiet /etc/cron.d/rpk-status.cron &
                exec rpk start --advertise-kafka-addr $kafkaURL $@
                """.trimIndent()
            // val startCommand = "rpk start --advertise-kafka-addr $kafkaURL"
            copyFileToContainer(
                Transferable.of(startCommand.toByteArray(StandardCharsets.UTF_8), 511), // octal 777
                STARTER_SCRIPT
            )
            // "start", "--advertise-kafka-addr", "kafka:9092"
        }
    }
    var container: KGenericContainer = KGenericContainer(image)
        .apply { withExposedPorts(port) }
        // .apply { var self = this; port.toTypedArray().forEach { self = withExposedPorts(it) }; self }
        .apply { withEnv(env) }
    // .apply { var self = this; if(commands!=null) { self = withCommand(command)}; self }
    // .apply {val cmds: Array<String> = arrayOf(*commands); withCommand(arrayOf("a")) }
    // .apply { if(customize!=null) customize(this) else this}
    // var host: String
    var exposedPort: Int = -1
    init {
        container.start()
        // host = container.host ?: "localhost"
        // val bindings = container.portBindings
        // val testHost = container.testHostIpAddress
        exposedPort = container.firstMappedPort ?: -1
    }
    fun close() {
        container.close()
    }
}
