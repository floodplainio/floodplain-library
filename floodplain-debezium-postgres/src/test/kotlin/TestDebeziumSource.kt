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
package io.floodplain.debezium.postgres

import java.util.UUID
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Test
import org.testcontainers.containers.GenericContainer

class InstantiatedContainer(image: String, port: Int, env: Map<String, String> = emptyMap()) {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var container: KGenericContainer?
    var host: String
    var exposedPort: Int = -1
    init {
        container = KGenericContainer(image)
            .apply { withExposedPorts(port) }
            .apply { withEnv(env) }
        container?.start()
        host = container?.host ?: "localhost"
        exposedPort = container?.firstMappedPort ?: -1
    }
    fun close() {
        container?.close()
    }
}
class TestDebeziumSource {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)

    @Test
    fun testShortRun() {
        // val offsets = File.createTempFile("temp", null).toPath()
        runBlocking {
            postgresDataSource("mypostgres", postgresContainer.host, postgresContainer.exposedPort, "dvdrental", "postgres", "mysecretpassword", UUID.randomUUID().toString(),
                emptyMap())
                .take(500)
                .collect { it.key
                    println("Topic: ${it.topic} key: ${it.key}")
                }
            println("completed")
        }
    }

    @After
    fun shutdown() {
        postgresContainer.close()
    }
}
