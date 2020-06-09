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
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.remotejoin.StoreStateProcessor
import java.math.BigDecimal
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.apache.kafka.streams.state.KeyValueStore

private val logger = mu.KotlinLogging.logger {}

class TestTopology {

    @Test
        /**
         * Test the simplest imaginable pipe: One source and one sink.
         */
    fun testSimple() {
        stream {
            source("@sometopic") {
                sink("@outputTopic")
            }
        }.renderAndTest {
            input("@sometopic", "key1", empty().set("name", "gorilla"))
            input("@sometopic", "key1", empty().set("name", "monkey"))
            assertEquals("gorilla", output("@outputTopic").second["name"])
            assertEquals("monkey", output("@outputTopic").second["name"])
        }
    }

    @Test
    fun testDelete() {
        stream("somegen") {
            source("@sometopic") {
                sink("@outputtopic")
            }
        }.renderAndTest {
            input("@sometopic", "key1", empty().set("name", "gorilla"))
            delete("@sometopic", "key1")
            output("@outputtopic")
            deleted("@outputtopic").equals("key1")
            logger.info("Topic now empty: ${isEmpty("@outputtopic")}")
        }
    }

    @Test
    fun simpleTransformation() {
        stream {
            source("mysource") {
                set {
                    _, primary, _ -> primary.set("name", "Frank")
                }
                sink("people")
            }
        }.renderAndTest {
            input("mysource", "1", empty().set("species", "human"))
            println("outputs: ${outputs()}")
            val (_, value) = output("people")
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
        }.renderAndTest {
            assertTrue(isEmpty("@output"))
            input("@left", "key1", empty().set("name", "left1"))
            assertTrue(isEmpty("@output"))
            input("@left", "wrongkey", empty().set("name", "nomatter"))
            assertTrue(isEmpty("@output"))
            input("@right", "key1", empty().set("subname", "monkey"))
            val (_, result) = output("@output")
            logger.info("Result: $result")
            assertEquals("monkey", result["rightsub/subname"])
            delete("@left", "key1")
            // TODO add tests to check store sizes
            assertEquals("key1", deleted("@output"))
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
        }.renderAndTest {
            assertTrue(isEmpty("@output"))
            val msg = empty().set("name", "left1")
            input("@left", "key1", msg)
            assertTrue(!isEmpty("@output"))
            assertEquals(output("@output").second, msg)
            input("@left", "otherkey", empty().set("name", "nomatter"))
            assertTrue(!isEmpty("@output"))
            assertEquals(output("@output").second, empty().set("name", "nomatter"))
            input("@right", "key1", empty().set("subname", "monkey"))
            val (_, result) = output("@output")
            logger.info("Result: $result")
            assertEquals("monkey", result["rightsub/subname"])
            delete("@left", "key1")
            assertEquals("key1", deleted("@output"))
        }
    }

    @Test
    fun testGroup() {
        stream("somegen") {
            source("src") {
                group { message -> message["subkey"] as String }
                sink("mysink")
            }
        }.renderAndTest {
            val record1 = empty().set("subkey", "subkey1")
            val record2 = empty().set("subkey", "subkey2")
            input("src", "key1", record1)
            input("src", "key2", record2)
            val (k1, v1) = output("mysink")
            assertEquals("subkey1|key1", k1)
            assertEquals(record1, v1)
            val (k2, v2) = output("mysink")
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
        }.renderAndTest {
            assertTrue(isEmpty("@output"))
            val leftRecord = empty().set("name", "left1")
            input("@left", "key1", leftRecord)
            assertTrue(!isEmpty("@output"))
            val record1 = empty().set("foreignkey", "key1").set("recorddata", "data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata", "data2")
            input("@right", "otherkey1", record1)
            input("@right", "otherkey2", record2)

            val (key, value) = output("@output")
            assertEquals("key1", key)
            val sublist: List<IMessage> = (value["rightsub"] ?: emptyList<IMessage>()) as List<IMessage>
            assertTrue(sublist.isEmpty())

            val outputs = outputSize("@output")
            assertEquals(2, outputs, "should have 2 elements")
            output("@output") // skip one
            val (_, v3) = output("@output")
            val subList = v3.get("rightsub") as List<*>
            assertEquals(2, subList.size)
            assertEquals(record1, subList[0])
            assertEquals(record2, subList[1])
            delete("@right", "otherkey1")
            val (_, v4) = output("@output")
            val subList2 = v4.get("rightsub") as List<*>
            assertEquals(1, subList2.size)
            delete("@right", "otherkey2")
            val (_, v5) = output("@output")
            val subList3 = v5.get("rightsub") as List<*>
            assertEquals(0, subList3.size)
            delete("@left", "key1")
            assertEquals("key1", deleted("@output"))
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
        }.renderAndTest {
            assertTrue(isEmpty("@output"))
            val leftRecord = empty().set("name", "left1")
            input("@left", "key1", leftRecord)
//            assertTrue(!isEmpty("@output"))

            val record1 = empty().set("foreignkey", "key1").set("recorddata", "data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata", "data2")
            input("@right", "otherkey1", record1)
            input("@right", "otherkey2", record2)

            // TODO skip the 'ghost delete' I'm not too fond of this, this one should be skippable
            deleted("@output")
            val outputs = outputSize("@output")
            assertEquals(2, outputs, "should have 2 elements")
            output("@output") // skip one
            val (_, v3) = output("@output")
            val subList = v3.get("rightsub") as List<*>
            assertEquals(2, subList.size)
            assertEquals(record1, subList[0])
            assertEquals(record2, subList[1])
            delete("@right", "otherkey1")
            val (_, v4) = output("@output")
            val subList2 = v4.get("rightsub") as List<*>
            assertEquals(1, subList2.size)
            delete("@right", "otherkey2")
            assertEquals("key1", deleted("@output"))
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
        }.renderAndTest {
            input("@source", "key1", empty().set("name", "myname"))
            input("@source", "key2", empty().set("name", "notmyname"))
            input("@source", "key3", empty().set("name", "myname"))
            assertEquals(2, outputSize("@output"))
//            val (key,value) = output("@source")
        }
    }

    @Test
    fun testSimpleScan() {
        stream {
            source("@source") {
                scan({ msg -> msg["total"] = 0; msg }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                })
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }

                sink("@output")
            }
        }.renderAndTest {
            input("@source", "key1", empty())
            input("@source", "key1", empty())
            output("@output") // initial key, total = 1
            output("@output") // delete previous key, total = 0
            val (key, value) = output("@output") // insert key again, total = 1
            assertTrue(outputSize("@output") == 0L)
            logger.info("Key: $key Value: $value")
            assertEquals(StoreStateProcessor.COMMONKEY, key)
            assertEquals(1, value["total"], "Entries with the same key should replace")
        }
    }

    @Test
    fun testSimpleScanWithDecimals() {
        stream {
            source("@source") {
                scan({
                        _ -> empty().set("total", BigDecimal.valueOf(0))
                }, {
                    set {
                        _, _, acc -> acc["total"] = (acc["total"] as BigDecimal).add(BigDecimal.valueOf(1))
                        acc
                    }
                }, {
                    set { _, _, acc ->
                        if (acc["total"] != null) {
                            println("A: $acc")
                        }
                        acc["total"] = (acc["total"] as BigDecimal).subtract(BigDecimal.valueOf(1))
                        acc
                    }
                })
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }
                sink("@output")
            }
        }.renderAndTest {
            input("@source", "key1", empty().set("message", "message1"))
            input("@source", "key1", empty().set("message", "message1"))
            output("@output") // initial key, total = 1
            output("@output") // delete previous key, total = 0
            val (key, value) = output("@output") // insert key again, total = 1
            assertTrue(outputSize("@output") == 0L)
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
                scan({ msg -> msg["groupKey"] as String }, { msg -> msg["total"] = 0; msg }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                })
                each { key, msg, acc -> logger.info("Each: $key -> $msg -> $acc") }

                sink("@output")
            }
        }.renderAndTest {
            input("@source", "key1", empty().set("groupKey", "group1"))
            input("@source", "key1", empty().set("groupKey", "group1"))
            output("@output") // initial key, total = 1
            output("@output") // delete previous key, total = 0
            val (_, value) = output("@output") // insert key again, total = 1
            assertTrue(outputSize("@output") == 0L)
            logger.info("Value: $value")
            assertEquals(1, value["total"], "Entries with the same key should replace")
            delete("@source", "key1")
            val (groupkey, afterDelete) = output("@output") // key1 deleted, so total should be 0 again
            assertEquals("group1", groupkey)
            assertEquals(0, afterDelete["total"])
            input("@source", "key1", empty().set("groupKey", "group1"))
            input("@source", "key2", empty().set("groupKey", "group1"))
            output("@output")

            val (_, ovalue) = output("@output")
            logger.info("Value: $ovalue")
            assertEquals(2, ovalue["total"], "Entries with different keys should add")
            input("@source", "key1", empty().set("groupKey", "group1"))
            delete("@source", "key1")
            val (_, ivalue) = output("@output")
            logger.info("Value:> $ivalue")
            delete("@source", "key2")

            assertEquals(1, ivalue["total"], "Entries with different keys should add")
            skip("@output", 2)
            val (_, value2) = output("@output")
            logger.info("Value: $value2 outputsize: ${outputSize("@output")}")
            assertEquals(0, value2["total"], "Delete should subtract")
        }
    }

    @Test
    fun testScanGroupedSimple() {
        stream {
            source("@source") {
                scan({ msg -> msg["groupKey"] as String }, { msg -> msg["total"] = 0; msg }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int + 1; acc }
                }, {
                    set { _, _, acc -> acc["total"] = acc["total"] as Int - 1; acc }
                })
                each {
                        key, msg, acc -> logger.info("Each: $key -> $msg -> $acc")
                }

                sink("@output")
            }
        }.renderAndTest {
            input("@source", "key1", empty().set("groupKey", "group1"))
            delete("@source", "key1")
            skip("@output", 1)
            val (_, value) = output("@output") // key1 deleted, so total should be 0 again
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
        }.renderAndTest {
            input("@source", "key1", empty().set("category", "category1"))
            assertEquals(1, outputSize("@category1"))
            assertEquals(0, outputSize("@category2"))
            input("@source", "key2", empty().set("category", "category2"))
            assertEquals(2, outputSize("@all"))
            assertEquals(2, outputSize("@sink"))
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
        }.renderAndTest {
            input("@source", "key1", empty().set("destination", "mydestination"))
            input("@source", "key1", empty().set("destination", "otherdestination"))
            assertEquals(1, outputSize("mydestination"))
            assertEquals(1, outputSize("otherdestination"))
        }
    }

    @Test
    fun testDiff() {
        stream {
            source("@source") {
                diff()
                sink("@output")
            }
        }.renderAndTest {

            val stateStore = stateStore(topologyContext().topicName("@diff_1_1"))
            input("@source", "key1", empty().set("value", "value1"))
            input("@source", "key1", empty().set("value", "value1"))
            assertEquals(1, outputSize("@output"))
            input("@source", "key2", empty().set("value", "value1"))
            assertEquals(2, outputSize("@output"))
            input("@source", "key1", empty().set("value", "value2"))
            assertEquals(3, outputSize("@output"))
            assertEquals(2, countStateStoreSize(stateStore))
            delete("@source", "key1")
            assertEquals(4, outputSize("@output"))
            delete("@source", "key2")
            assertEquals(5, outputSize("@output"))
            getStateStoreNames().forEach { k ->
                logger.info("Key: $k")
            }
            stateStore.flush()
            assertEquals(0, countStateStoreSize(stateStore))
        }
    }

    private fun countStateStoreSize(store: KeyValueStore<String, ReplicationMessage>): Long {
        var i = 0L
        store.all().forEach { i++ }
        return i
    }

    @Test
    fun testBuffer() {
        stream {
            source("@source") {
                buffer(Duration.ofSeconds(9), 10)
                sink("@output")
            }
        }.renderAndTest {
            val msg = empty().set("value", "value1")
            input("@source", "key1", msg)
            // shouldn't have arrived yet:
            assertTrue(isEmpty("@output"))
            // advance time
            advanceWallClockTime(Duration.ofSeconds(15))
            // should have result:
            assertTrue(!isEmpty("@output"))
            // same message:
            assertEquals(msg, output("@output").second)
            // now make sure only one gets through
            val otherMsg = empty().set("value", "value2")
            input("@source", "key1", msg)
            input("@source", "key1", otherMsg)
            advanceWallClockTime(Duration.ofSeconds(15))
            assertEquals(1, outputSize("@output"))
            assertEquals(otherMsg, output("@output").second)
            // now check size restriction. Max size is 10. Insert 20. expect 10 to come out.
            for (i in 0..19) {
                input("@source", "newkey$i", empty().set("value", "value$i"))
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
}
