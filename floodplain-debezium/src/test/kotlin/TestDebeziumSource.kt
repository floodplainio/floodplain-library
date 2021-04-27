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

import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import io.floodplain.ChangeRecord
import io.floodplain.test.InstantiatedContainer
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Ignore
import org.junit.Test
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals

class TestDebeziumSource {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)

    @Test
    fun testShortRun() {
        runBlocking {
            val resultList = mutableListOf<ChangeRecord>()
            createDebeziumChangeFlow(
                "mypostgres", "io.debezium.connector.postgresql.PostgresConnector", postgresContainer.host, postgresContainer.exposedPort, "dvdrental", "postgres", "mysecretpassword", UUID.randomUUID().toString(),
                emptyMap()
            )
                .onEach { println("message: ${it.key} topic: ${it.topic}") }
                .take(500)
                .toList(resultList)
            assertEquals(500, resultList.size)
        }
    }

    @Test @Ignore // just a doodle, could remove
    fun testEmbedded() {

// Define the configuration for the Debezium Engine with MySQL connector...

// Define the configuration for the Debezium Engine with MySQL connector...
        val props = Properties()

        // , postgresContainer.exposedPort
        props.setProperty("name", "engine")
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat")
        props.setProperty("offset.flush.interval.ms", "60000")
        props.setProperty("database.dbname", "dvdrental")
/* begin connector properties */
/* begin connector properties */props.setProperty("database.hostname", postgresContainer.host)
        props.setProperty("database.port", "${postgresContainer.exposedPort}")
        props.setProperty("database.user", "postgres")
        props.setProperty("database.password", "mysecretpassword")
        props.setProperty("database.server.id", "85744")
        props.setProperty("database.server.name", "my-app-connector")
        props.setProperty(
            "database.history",
            "io.debezium.relational.history.FileDatabaseHistory"
        )
        props.setProperty(
            "database.history.file.filename",
            "dbhistory.dat"
        )

// Create the engine with this configuration ...
        val engine = DebeziumEngine.create(Json::class.java)
            .using(props)
            .notifying { record -> println(record) }.build()
        engine.run()
// Engine is stopped when the main code is finished
    }

    @After
    fun shutdown() {
        postgresContainer.close()
    }
}
