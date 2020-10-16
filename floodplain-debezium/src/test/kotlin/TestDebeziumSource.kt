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

import io.floodplain.ChangeRecord
import io.floodplain.test.InstantiatedContainer
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Test
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
                .take(500)
                .toList(resultList)
            assertEquals(500, resultList.size)
        }
    }

    @After
    fun shutdown() {
        postgresContainer.close()
    }
}
