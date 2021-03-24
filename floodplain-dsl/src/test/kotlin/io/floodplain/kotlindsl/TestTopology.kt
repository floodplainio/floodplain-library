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
package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.sink.logSink
import io.floodplain.kotlindsl.sink.logSinkConfig
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.api.Topic
import io.floodplain.streams.debezium.JSONToReplicationMessage
import io.floodplain.streams.remotejoin.StoreStateProcessor
import kotlinx.coroutines.delay
import org.apache.kafka.streams.state.KeyValueStore
import java.math.BigDecimal
import java.time.Duration
import java.util.Optional
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

private val logger = mu.KotlinLogging.logger {}

@Suppress("UNCHECKED_CAST")
class TestTopology {

    @Test
    /**
     * Test the simplest imaginable pipe: One source and one sink.
     */
    fun testSimple() {
        stream("thing") {
            from("sometopic") {
                sinkQualified("outputTopic")
            }
        }.renderAndExecute {
            inputQualified("sometopic", "key1", empty().set("name", "gorilla"))
            inputQualified("sometopic", "key1", empty().set("name", "monkey"))
            assertEquals("gorilla", outputQualified("outputTopic").second["name"])
            assertEquals("monkey", outputQualified("outputTopic").second["name"])
        }
    }

    private fun testQualifiedWithTenantAndDeployment(tenant: String?, deployment: String?, generation: String?) {
        stream(tenant,deployment,generation?:"defaultGeneration") {
            from("sometopic") {
                sinkQualified("outputTopic")
            }
        }.renderAndExecute {
            inputQualified("sometopic", "key1", empty().set("name", "gorilla"))
            inputQualified("sometopic", "key1", empty().set("name", "monkey"))
            assertEquals("gorilla", outputQualified("outputTopic").second["name"])
            assertEquals("monkey", outputQualified("outputTopic").second["name"])
        }
    }

    private fun testNonQualifiedWithTenantAndDeployment(tenant: String?, deployment: String?, generation: String?) {
        stream(tenant,deployment, generation?: "defaultGeneration") {
            source("@sometopic") {
                sink("@outputTopic")
            }
        }.renderAndExecute {
            input(generationalTopic("sometopic"), "key1", empty().set("name", "gorilla"))
            input(generationalTopic("sometopic"), "key1", empty().set("name", "monkey"))
            assertEquals("gorilla", output(generationalTopic("outputTopic")).second["name"])
            assertEquals("monkey", output(generationalTopic("outputTopic")).second["name"])
        }
    }

    @Test
    fun testQualifications() {

        // Test both qualified and non-qualified topic resolution.
        // I intend to drop non-qualified in the future
        testNonQualifiedWithTenantAndDeployment(null,null,null)
        testNonQualifiedWithTenantAndDeployment("ten",null,null)
        testNonQualifiedWithTenantAndDeployment("ten","dep",null)
        testNonQualifiedWithTenantAndDeployment("ten","dep","somegeneration")

        testQualifiedWithTenantAndDeployment(null,null,null)
        testQualifiedWithTenantAndDeployment("ten",null,null)
        testQualifiedWithTenantAndDeployment("ten","dep",null)
        testQualifiedWithTenantAndDeployment("ten","dep","somegeneration")
    }
    @Test
    fun testDelete() {
        stream("somegen") {
            source("@sometopic") {
                sink("@outputtopic")
            }
        }.renderAndExecute {
            input(generationalTopic("sometopic"), "key1", empty().set("name", "gorilla"))
            delete(generationalTopic("sometopic"), "key1")
            output(generationalTopic("outputtopic"))
            assertTrue(deleted(generationalTopic("outputtopic")) == "key1", "Key mismatch")
            logger.info("Topic now empty: ${isEmpty(qualifiedTopic("outputtopic"))}")
        }
    }

    @Test
    fun simpleTransformation() {
        stream {
            source("mysource") {
                set {
                    _, primary, _ ->
                    primary.set("name", "Frank")
                }
                sink("people")
            }
        }.renderAndExecute {
            inputQualified("mysource", "1", empty().set("species", "human"))
            logger.info("outputs: ${outputs()}")
            val (_, value) = output(topic("people"))
            logger.info("Person found: $value")
        }
    }
    @Test
    fun testSimpleJoin() {
        stream("somegen") {
            source("@left") {
                join {
                    source("@right") {}
                }
                set { _, left, right ->
                    left["rightsub"] = right
                    left
                }
                sink("@output")
            }
        }.renderAndExecute {
            assertTrue(isEmpty(qualifiedTopic("output")))
            input(generationalTopic("left"), "key1", empty().set("name", "left1"))
            assertTrue(isEmpty(qualifiedTopic("output")))
            input(generationalTopic("left"), "wrongkey", empty().set("name", "nomatter"))
            assertTrue(isEmpty(qualifiedTopic("output")))
            input(generationalTopic("right"), "key1", empty().set("subname", "monkey"))
            val (_, result) = output(generationalTopic("output"))
            logger.info("Result: $result")
            assertEquals("monkey", result["rightsub/subname"])
            delete(generationalTopic("left"), "key1")
            // TODO add tests to check store sizes
            assertEquals("key1", deleted(generationalTopic("output")))
        }
    }

    @Test
    fun testOptionalSingleJoin() {
        stream("somegen") {
            source("@left") {
                join(optional = true) {
                    source("@right") {
                    }
                }
                set { _, left, right ->
                    left["rightsub"] = right
                    left
                }
                sink("@output")
            }
        }.renderAndExecute {
            assertTrue(isEmpty(generationalTopic("output")))
            val msg = empty().set("name", "left1")
            input(generationalTopic("left"), "key1", msg)
            assertTrue(!isEmpty(generationalTopic("output")))
            assertEquals(output(generationalTopic("output")).second, msg)
            input(generationalTopic("left"), "otherkey", empty().set("name", "nomatter"))
            assertTrue(!isEmpty(generationalTopic("output")))
            assertEquals(output(generationalTopic("output")).second, empty().set("name", "nomatter"))
            input(generationalTopic("right"), "key1", empty().set("subname", "monkey"))
            val (_, result) = output(generationalTopic("output"))
            logger.info("Result: $result")
            assertEquals("monkey", result["rightsub/subname"])
            delete(generationalTopic("left"), "key1")
            assertEquals("key1", deleted(generationalTopic("output")))
        }
    }

    @Test
    fun testMultipleSinks() {
        stream("somegen") {
            source("src") {
                group { message -> message["subkey"] as String }
                externalSink("mysink")
                externalSink("myothersink")
            }
        }.renderAndExecute {
            val record1 = empty().set("subkey", "subkey1")
            val record2 = empty().set("subkey", "subkey2")
            input(topic("src"), "key1", record1)
            input(topic("src"), "key2", record2)
            assertEquals(2, outputSize(topic("mysink")), "Expected two messages in topic")
            assertEquals(2, outputSize(topic("myothersink")), "Expected two messages in topic")
        }
    }
    @Test
    fun testGroup() {
        stream("somegen") {
            source("src") {
                group { message -> message["subkey"] as String }
                sink("mysink")
            }
        }.renderAndExecute {
            val record1 = empty().set("subkey", "subkey1")
            val record2 = empty().set("subkey", "subkey2")
            input(topic("src"), "key1", record1)
            input(topic("src"), "key2", record2)
            val (k1, v1) = output(topic("mysink"))
            assertEquals("subkey1|key1", k1)
            assertEquals(record1, v1)
            val (k2, v2) = output(topic("mysink"))
            assertEquals("subkey2|key2", k2)
            assertEquals(record2, v2)

            // TODO continue
        }
    }

    @Test
    fun testSingleToManyJoinOptional() {
        stream("somegen") {
            source("@left") {
                joinGrouped(optional = true, debug = true) {
                    source("@right") {
                        group { msg -> msg["foreignkey"] as String }
                    }
                }
                set { _, left, right ->
                    left["rightsub"] = right["list"]
                    left
                }
                each { key, left, right ->
                    logger.info("Message: $left RightMessage $right key: $key")
                }
                sink("@output")
            }
        }.renderAndExecute {
            assertTrue(isEmpty(generationalTopic("output")))
            val leftRecord = empty().set("name", "left1")
            input(generationalTopic("left"), "key1", leftRecord)
            assertTrue(!isEmpty(generationalTopic("output")))
            val record1 = empty().set("foreignkey", "key1").set("recorddata", "data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata", "data2")
            input(generationalTopic("right"), "otherkey1", record1)
            input(generationalTopic("right"), "otherkey2", record2)

            val (key, value) = output(generationalTopic("output"))
            assertEquals("key1", key)
            val sublist: List<IMessage> = (value["rightsub"] ?: emptyList<IMessage>()) as List<IMessage>
            assertTrue(sublist.isEmpty())

            val outputs = outputSize(generationalTopic("output"))
            assertEquals(2, outputs, "should have 2 elements")
            output(generationalTopic("output")) // skip one
            val (_, v3) = output(generationalTopic("output"))
            val subList = v3["rightsub"] as List<*>
            assertEquals(2, subList.size)
            assertEquals(record1, subList[0])
            assertEquals(record2, subList[1])
            delete(generationalTopic("right"), "otherkey1")
            val (_, v4) = output(generationalTopic("output"))
            val subList2 = v4["rightsub"] as List<*>
            assertEquals(1, subList2.size)
            delete(generationalTopic("right"), "otherkey2")
            val (_, v5) = output(generationalTopic("output"))
            val subList3 = v5["rightsub"] as List<*>?
            assertEquals(0, subList3?.size ?: 0)
            delete(generationalTopic("left"), "key1")
            assertEquals("key1", deleted(generationalTopic("output")))
        }
    }

    @Test
    fun testSingleToManyJoin() {
        stream("somegen") {
            source("@left") {
                joinGrouped(debug = true) {
                    source("@right") {
                        group { msg -> msg["foreignkey"] as String }
                    }
                }
                set { _, left, right ->
                    left["rightsub"] = right["list"]
                    left
                }
                each { key, left, right ->
                    logger.info("Message: $left RightMessage $right key: $key")
                }
                sink("@output")
            }
        }.renderAndExecute {
            assertTrue(isEmpty(qualifiedTopic("output")))
            val leftRecord = empty().set("name", "left1")
            input(generationalTopic("left"), "key1", leftRecord)
//            assertTrue(!isEmpty("@output"))

            val record1 = empty().set("foreignkey", "key1").set("recorddata", "data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata", "data2")
            input(generationalTopic("right"), "otherkey1", record1)
            input(generationalTopic("right"), "otherkey2", record2)

            // TODO skip the 'ghost delete' I'm not too fond of this, this one should be skippable
            deleted(generationalTopic("output"))
            val outputs = outputSize(generationalTopic("output"))
            assertEquals(2, outputs, "should have 2 elements")
            output(generationalTopic("output")) // skip one
            val (_, v3) = output(generationalTopic("output"))
            val subList = v3["rightsub"] as List<*>
            assertEquals(2, subList.size)
            assertEquals(record1, subList[0])
            assertEquals(record2, subList[1])
            delete(generationalTopic("right"), "otherkey1")
            val (_, v4) = output(generationalTopic("output"))
            val subList2 = v4["rightsub"] as List<*>
            assertEquals(1, subList2.size)
            delete(generationalTopic("right"), "otherkey2")
            assertEquals("key1", deleted(generationalTopic("output")))
        }
    }

    @Test
    fun testFilter() {
        stream("anygen") {
            source("@source") {
                filter { _, value ->
                    value["name"] == "myname"
                }
                sink("@output")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("name", "myname"))
            input(generationalTopic("source"), "key2", empty().set("name", "notmyname"))
            input(generationalTopic("source"), "key3", empty().set("name", "myname"))
            assertEquals(2, outputSize(generationalTopic("output")))
//            val (key,value) = output("@source")
        }
    }

    @Test
    fun testSimpleScan() {
        stream {
            source("@source") {
                scan(
                    { msg -> msg["total"] = 0; msg },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                    },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                    }
                )
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }

                sink("@output")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty())
            input(generationalTopic("source"), "key1", empty())
            output(generationalTopic("output")) // initial key, total = 1
            output(generationalTopic("output")) // delete previous key, total = 0
            val (key, value) = output(generationalTopic("output")) // insert key again, total = 1
            assertTrue(outputSize(generationalTopic("output")) == 0L)
            logger.info("Key: $key Value: $value")
            assertEquals(StoreStateProcessor.COMMONKEY, key)
            assertEquals(1, value["total"], "Entries with the same key should replace")
        }
    }

    @Test
    fun testSimpleScanWithDecimals() {
        stream {
            source("@source") {
                scan(
                    {
                        empty().set("total", BigDecimal.valueOf(0))
                    },
                    {
                        set {
                            _, _, acc ->
                            acc["total"] = (acc["total"] as BigDecimal).add(BigDecimal.valueOf(1))
                            acc
                        }
                    },
                    {
                        set { _, _, acc ->
                            if (acc["total"] != null) {
                                logger.info("A: $acc")
                            }
                            acc["total"] = (acc["total"] as BigDecimal).subtract(BigDecimal.valueOf(1))
                            acc
                        }
                    }
                )
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }
                sink("@output")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("message", "message1"))
            input(generationalTopic("source"), "key1", empty().set("message", "message1"))
            output(generationalTopic("output")) // initial key, total = 1
            output(generationalTopic("output")) // delete previous key, total = 0
            val (key, value) = output(generationalTopic("output")) // insert key again, total = 1
            assertTrue(outputSize(generationalTopic("output")) == 0L)
            logger.info("Key: $key Value: $value")
            assertEquals(StoreStateProcessor.COMMONKEY, key)
            assertEquals(BigDecimal(1), value["total"], "Entries with the same key should replace")
        }
    }
    // TODO Can we remove the extra 'block' braces?
    // TODO Can we implement += and ++ like operators?
    // TODO Introduce 'eachDelete(key)
    @Test
    fun testScan() {
        stream {
            source("@source") {
                scan(
                    { msg -> msg["groupKey"] as String },
                    { msg -> msg["total"] = 0; msg },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                    },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                    }
                )
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }

                sink("@output")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("groupKey", "group1"))
            input(generationalTopic("source"), "key1", empty().set("groupKey", "group1"))
            output(generationalTopic("output")) // initial key, total = 1
            output(generationalTopic("output")) // delete previous key, total = 0
            val (_, value) = output(generationalTopic("output")) // insert key again, total = 1
            assertTrue(outputSize(generationalTopic("output")) == 0L)
            logger.info("Value: $value")
            assertEquals(1, value["total"], "Entries with the same key should replace")
            delete(generationalTopic("source"), "key1")
            val (groupkey, afterDelete) = output(generationalTopic("output")) // key1 deleted, so total should be 0 again
            assertEquals("group1", groupkey)
            assertEquals(0, afterDelete["total"])
            input(generationalTopic("source"), "key1", empty().set("groupKey", "group1"))
            input(generationalTopic("source"), "key2", empty().set("groupKey", "group1"))
            output(generationalTopic("output"))

            val (_, ovalue) = output(generationalTopic("output"))
            logger.info("Value: $ovalue")
            assertEquals(2, ovalue["total"], "Entries with different keys should add")
            input(generationalTopic("source"), "key1", empty().set("groupKey", "group1"))
            delete(generationalTopic("source"), "key1")
            val (_, ivalue) = output(generationalTopic("output"))
            logger.info("Value:> $ivalue")
            delete(generationalTopic("source"), "key2")

            assertEquals(1, ivalue["total"], "Entries with different keys should add")
            skip(generationalTopic("output"), 2)
            val (_, value2) = output(generationalTopic("output"))
            logger.info("Value: $value2 outputsize: ${outputSize(generationalTopic("output"))}")
            assertEquals(0, value2["total"], "Delete should subtract")
        }
    }

    @Test
    fun testScanGroupedSimple() {
        stream {
            source("@source") {
                scan(
                    { msg -> msg["groupKey"] as String },
                    { msg -> msg["total"] = 0; msg },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                    },
                    {
                        set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                    }
                )
                each {
                    key, msg, acc ->
                    logger.info("Each: $key -> $msg -> $acc")
                }

                sink("@output")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("groupKey", "group1"))
            delete(generationalTopic("source"), "key1")
            skip(generationalTopic("output"), 1)
            val (_, value) = output(generationalTopic("output")) // key1 deleted, so total should be 0 again
            assertEquals(0, value["total"], "Entries with the same key should replace")
        }
    }

    @Test
    fun testFork() {
        stream {
            source("@source") {
                fork(
                    {
                        filter { _, value -> value["category"] == "category1" }
                        sink("@category1")
                    },
                    {
                        filter { _, value -> value["category"] == "category2" }
                        sink("@category2")
                    },
                    {
                        sink("@all")
                    }
                )
                sink("@sink")
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("category", "category1"))
            assertEquals(1, outputSize(generationalTopic("category1")))
            assertEquals(0, outputSize(generationalTopic("category2")))
            input(generationalTopic("source"), "key2", empty().set("category", "category2"))
            assertEquals(2, outputSize(generationalTopic("all")))
            assertEquals(2, outputSize(generationalTopic("sink")))
        }
    }

    @Test
    fun testDynamicSink() {
        stream {
            source("@source") {
                dynamicSink("somesink") { _, value ->
                    value["destination"] as String
                }
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "key1", empty().set("destination", "mydestination"))
            input(generationalTopic("source"), "key1", empty().set("destination", "otherdestination"))
            assertEquals(1, outputSize(topic("mydestination")))
            assertEquals(1, outputSize(topic("otherdestination")))
        }
    }

    @Test
    fun testReplicationParserForDateTime() {
        val data = javaClass.classLoader.getResource("decimalwithscale.json")?.readBytes()
            ?: throw IllegalArgumentException("Missing json file for decimalwithscale.json")
        val replicationMessage = JSONToReplicationMessage.processDebeziumBody(data, Optional.empty());
        val date = replicationMessage.value("payment_date")
        assertNotNull(date)

    }

    @Test
    fun testRawInput() {
        val data = javaClass.classLoader.getResource("decimalwithscale.json")?.readBytes()
            ?: throw IllegalArgumentException("Missing json file for decimalwithscale.json")
        stream {
            externalSource("source", Topic.FloodplainKeyFormat.FLOODPLAIN_STRING, Topic.FloodplainBodyFormat.CONNECT_JSON) {
                sinkQualified("sinktopic")
            }
        }.renderAndExecute {
            input(qualifiedTopic("source"), "key1".toByteArray(), data)
            val (_, value) = output(qualifiedTopic("sinktopic"))
            logger.info("value: $value")
            val amount = value.decimal("amount")
            assertEquals(BigDecimal.valueOf(299, 2), amount)
        }
    }

    @Test
    fun testDiff() {
        stream {
            source("@source") {
                diff()
                sink("@output")
            }
        }.renderAndExecute {

            val stateStore = stateStore(topologyContext().topicName("@diff_1_1"))
            input(generationalTopic("source"), "key1", empty().set("value", "value1"))
            input(generationalTopic("source"), "key1", empty().set("value", "value1"))
            assertEquals(1, outputSize(generationalTopic("output")))
            input(generationalTopic("source"), "key2", empty().set("value", "value1"))
            assertEquals(2, outputSize(generationalTopic("output")))
            input(generationalTopic("source"), "key1", empty().set("value", "value2"))
            assertEquals(3, outputSize(generationalTopic("output")))
            assertEquals(2, countStateStoreSize(stateStore))
            delete(generationalTopic("source"), "key1")
            assertEquals(4, outputSize(generationalTopic("output")))
            delete(generationalTopic("source"), "key2")
            assertEquals(5, outputSize(generationalTopic("output")))
            getStateStoreNames().forEach { k ->
                logger.info("Key: $k")
            }
            stateStore.flush()
            assertEquals(0, countStateStoreSize(stateStore))
        }
    }

    private fun countStateStoreSize(store: KeyValueStore<String, ReplicationMessage>): Long {
        var i = 0L
        store.all().forEach { _ -> i++ }
        return i
    }

    @Test
    fun testLogSink() {
        stream {
            val logSinkConfig = logSinkConfig("logname")
            source("@source") {
                logSink("logSinkTest", "@output", logSinkConfig)
            }
        }.renderAndExecute {
            input(generationalTopic("source"), "somekey", empty().set("myKey", "myValue"))
            delay(200)
        }
    }

    @Test
    fun testBuffer() {
        stream {
            source("@source") {
                buffer(Duration.ofSeconds(9), 10)
                sink("@output")
            }
        }.renderAndExecute {
            val msg = empty().set("value", "value1")
            input(generationalTopic("source"), "key1", msg)
            // shouldn't have arrived yet:
            assertTrue(isEmpty(generationalTopic("output")))
            // advance time
            advanceWallClockTime(Duration.ofSeconds(15))
            // should have result:
            assertTrue(!isEmpty(generationalTopic("output")))
            // same message:
            assertEquals(msg, output(generationalTopic("output")).second)
            // now make sure only one gets through
            val otherMsg = empty().set("value", "value2")
            input(generationalTopic("source"), "key1", msg)
            input(generationalTopic("source"), "key1", otherMsg)
            advanceWallClockTime(Duration.ofSeconds(15))
            assertEquals(1, outputSize(generationalTopic("output")))
            assertEquals(otherMsg, output(generationalTopic("output")).second)
            // now check size restriction. Max size is 10. Insert 20. expect 10 to come out.
            for (i in 0..19) {
                input(generationalTopic("source"), "newkey$i", empty().set("value", "value$i"))
            }
            logger.info("statestores: ${getStateStoreNames()}")
            // quick check if I'm not making unnecessary stores
//            assertEquals(2,getStateStoreNames().size)
//            stateStore(getStateStoreNames().first()).flush()
            val storeSize = countStateStoreSize(stateStore(getStateStoreNames().first())) // stateStore(getStateStoreNames().first()).approximateNumEntries()
            // TODO test size limit, works slightly different than I expected, isn't using the statestore,
            // TODO investigate if there is some 'native' cache store

            logger.info("Store szie: $storeSize")
//            assertEquals(10L,storeSize)
        }
    }

    @Test
    fun testRawJsonInput() {
        val originalKey =
            """
                {
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "int32",
                                "optional": false,
                                "field": "film_id"
                            }
                        ],
                        "optional": false,
                        "name": "instance_mypostgres.public.film.Key"
                    },
                    "payload": {
                        "film_id": 965
                    }
                }                
            """
        val body =
            """
                {
                    "schema": {
                        "type": "struct",
                        "fields": [
                            {
                                "type": "struct",
                                "fields": [
                                    {
                                        "type": "int32",
                                        "optional": false,
                                        "field": "film_id"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "title"
                                    },
                                    {
                                        "type": "string",
                                        "optional": true,
                                        "field": "description"
                                    }
                                ],
                                "optional": true,
                                "name": "instance_mypostgres.public.film.Value",
                                "field": "before"
                            },
                            {
                                "type": "struct",
                                "fields": [
                                    {
                                        "type": "int32",
                                        "optional": false,
                                        "field": "film_id"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "title"
                                    },
                                    {
                                        "type": "string",
                                        "optional": true,
                                        "field": "description"
                                    }
                                ],
                                "optional": true,
                                "name": "instance_mypostgres.public.film.Value",
                                "field": "after"
                            },
                            {
                                "type": "struct",
                                "fields": [
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "version"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "connector"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "name"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": false,
                                        "field": "ts_ms"
                                    },
                                    {
                                        "type": "string",
                                        "optional": true,
                                        "name": "io.debezium.data.Enum",
                                        "version": 1,
                                        "parameters": {
                                            "allowed": "true,last,false"
                                        },
                                        "default": "false",
                                        "field": "snapshot"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "db"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "schema"
                                    },
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "table"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": true,
                                        "field": "txId"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": true,
                                        "field": "lsn"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": true,
                                        "field": "xmin"
                                    }
                                ],
                                "optional": false,
                                "name": "io.debezium.connector.postgresql.Source",
                                "field": "source"
                            },
                            {
                                "type": "string",
                                "optional": false,
                                "field": "op"
                            },
                            {
                                "type": "int64",
                                "optional": true,
                                "field": "ts_ms"
                            },
                            {
                                "type": "struct",
                                "fields": [
                                    {
                                        "type": "string",
                                        "optional": false,
                                        "field": "id"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": false,
                                        "field": "total_order"
                                    },
                                    {
                                        "type": "int64",
                                        "optional": false,
                                        "field": "data_collection_order"
                                    }
                                ],
                                "optional": true,
                                "field": "transaction"
                            }
                        ],
                        "optional": false,
                        "name": "instance_mypostgres.public.film.Envelope"
                    },
                    "payload": {
                        "before": null,
                        "after": {
                            "film_id": 778,
                            "title": "Secrets Paradise",
                            "description": "A Fateful Saga of a Cat And a Frisbee who must Kill a Girl in A Manhattan Penthouse"
                        },
                        "source": {
                            "version": "1.2.0.Final",
                            "connector": "postgresql",
                            "name": "instance-mypostgres",
                            "ts_ms": 1594564042739,
                            "snapshot": "true",
                            "db": "dvdrental",
                            "schema": "public",
                            "table": "film",
                            "txId": 751,
                            "lsn": 30998528,
                            "xmin": null
                        },
                        "op": "r",
                        "ts_ms": 1594564042739,
                        "transaction": null
                    }
                }
            """

        stream("aaa") {
            val logSinkConfig = logSinkConfig("logname")
            externalSource("external", Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON) {
                // logSink("somesink", "@output", logSinkConfig)
                sinkQualified("output")
            }
        }.renderAndExecute {
            input( qualifiedTopic("external"), originalKey.toByteArray(), body.toByteArray())
            val (key,value) = output(qualifiedTopic("output"))
            assertEquals("965",key)
            logger.info("Result: $value")
        }
    }

    @Test
    fun testArgumentParser() {
        stream {
            source("@sometopic") {
                sink("@outputTopic")
            }
        }.runWithArguments(arrayOf("--help")) {
        }
    }
}
