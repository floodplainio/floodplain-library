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
package io.floodplain.kotlindsl.example

import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.from
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.localMongoConfig
import io.floodplain.mongodb.toMongo
private val logger = mu.KotlinLogging.logger {}

fun main() {

    stream("KNBSB", "develop", "generation96") {
        val mongoConfig = localMongoConfig("$tenant-$deployment-$generation-mongosink", "mongodb://fp1", "$tenant-$deployment-$generation-mongodump")
        from("$tenant-$deployment-sportlinkkernel-CALENDARDAY") {
            each { key, main, _ ->
                logger.info("Key: $key message: $main")
            }
            toMongo("calendarday", "$tenant-$deployment-$generation-sometopic", mongoConfig)
        }
    }.renderAndSchedule(null, "10.8.0.7:9092")
}
