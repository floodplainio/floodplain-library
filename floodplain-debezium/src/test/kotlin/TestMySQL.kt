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

import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import io.floodplain.ChangeRecord
import io.floodplain.test.InstantiatedContainer
import org.junit.After
import org.junit.Before
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.deleteIfExists

private val logger = mu.KotlinLogging.logger {}

class TestMySQL {

    var offsetFilePath: Path? = null

    private val postgresContainer = InstantiatedContainer(
        "debezium/example-mysql:1.8.1.Final",
        3306,
        mapOf(
            "MYSQL_ROOT_PASSWORD" to "debezium",
            "MYSQL_USER" to "mysqluser",
            "MYSQL_PASSWORD" to "mysqlpw"
        )
    )

    private var engine: DebeziumEngine<ChangeEvent<String, String>>? = null
    private val itemCounter = AtomicInteger(0)

    @Before
    fun setup() {
        offsetFilePath = createOffsetFilePath()
    }

    @After
    fun teardown() {
        offsetFilePath?.deleteIfExists()
        offsetFilePath = null
    }

    @Test
    @Tag("integration")
    fun testSimpleMySqlRun() {
        // Find better way to configure this?
        System.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms", "1000")
        logger.info("Creating offset files at: $offsetFilePath")
        val props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("database.hostname", postgresContainer.host)
        props.setProperty("database.port", "${postgresContainer.exposedPort}")
        props.setProperty("database.server.name", "instance-mysqlsource")
        props.setProperty("database.dbname", "inventory")
        props.setProperty("database.user", "root")
        props.setProperty("database.password", "debezium")
        props.setProperty("database.server.id", "${System.currentTimeMillis()}")
        // props.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms","1000")
        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
        props.setProperty("database.history.file.filename", "currenthistory")
        engine = DebeziumEngine.create(Json::class.java)
            .using(props)
            .notifying { record ->
                send(
                    ChangeRecord(
                        record.destination(),
                        record.key(),
                        record.value()?.toByteArray()
                    )
                )
            }
            .build()
        engine?.run()
    }

    private fun send(changeRecord: ChangeRecord) {
        if (itemCounter.get() > 33) {
            logger.info("Closing engine")
            // delete history file:
            Files.deleteIfExists(Path.of("currenthistory"))
            engine?.close()
        }

        logger.info(
            "Record detected:${changeRecord.topic} ${changeRecord.key}" +
                " ${changeRecord.value} count: ${itemCounter.incrementAndGet()}"
        )
    }

    private fun createOffsetFilePath(offsetId: String? = null): Path {
        val tempFile = kotlin.io.path.createTempFile(offsetId ?: UUID.randomUUID().toString().substring(0, 7))
        return tempFile
    }
}
