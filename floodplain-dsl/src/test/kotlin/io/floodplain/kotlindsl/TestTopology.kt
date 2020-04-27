package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue


private val logger = mu.KotlinLogging.logger {}


class TestTopology {

    @Test
    fun testSimple() {
        pipe("somegen") {
            source("@sometopic") {
                sink("@outputTopic")
            }
        }.renderAndTest {
            it.input("@sometopic", "key1", empty().set("name", "gorilla"))
            it.input("@sometopic", "key1", empty().set("name", "monkey"))
            assertEquals("gorilla", it.output("@outputTopic").second["name"])
            assertEquals("monkey", it.output("@outputTopic").second["name"])
        }
    }



    @Test
    fun testDelete() {
        pipe("somegen") {
            source("@sometopic") {
                sink("@outputtopic")
            }
        }.renderAndTest {
            it.input("@sometopic", "key1", empty().set("name","gorilla"))
            it.delete("@sometopic","key1")
            it.output("@outputtopic")
            val eq = it.deleted("@outputtopic").equals("key1");
            logger.info("equal: $eq")
            logger.info ("Topic now empty: ${it.isEmpty("@outputtopic")}")
        }
    }


    @Test
    fun testSimpleJoin() {
        pipe("somegen") {
            source("@left") {
                join {
                    source("@right") {}
                }
                set { left, right ->
                    left["rightsub"] = right
                    left
                }
                sink("@output")
            }
        }.renderAndTest {
            assertTrue(it.isEmpty("@output"))
            it.input("@left", "key1", empty().set("name", "left1"))
            assertTrue(it.isEmpty("@output"))
            it.input("@left", "wrongkey", empty().set("name", "nomatter"))
            assertTrue(it.isEmpty("@output"))
            it.input("@right", "key1", empty().set("subname", "monkey"))
            val (_,result) = it.output("@output")
            logger.info("Result: $result")
            assertEquals("monkey",result["rightsub/subname"])
            it.delete("@left","key1")
            assertEquals("key1",it.deleted("@output"))
        }
    }

    @Test
    fun testOptionalSingleJoin() {
        pipe("somegen") {
            source("@left") {
                join(optional = true) {
                    source("@right") {

                    }
                }
                set { left, right ->
                    left["rightsub"] = right
                    left
                }
                sink("@output")
            }
        }.renderAndTest {
            assertTrue(it.isEmpty("@output"))
            val msg = empty().set("name", "left1")
            it.input("@left", "key1", msg)
            assertTrue(!it.isEmpty("@output"))
            assertEquals(it.output("@output").second, msg)
            it.input("@left", "otherkey", empty().set("name", "nomatter"))
            assertTrue(!it.isEmpty("@output"))
            assertEquals(it.output("@output").second, empty().set("name","nomatter"))
            it.input("@right", "key1", empty().set("subname", "monkey"))
            val (_,result) = it.output("@output")
            logger.info("Result: $result")
            assertEquals("monkey",result["rightsub/subname"])
            it.delete("@left","key1")
            assertEquals("key1",it.deleted("@output"))
        }
    }

    @Test
    fun testGroup() {
        pipe("somegen") {
            source("src") {
                group {message->message["subkey"] as String }
                sink("mysink")
            }
        }.renderAndTest {
            it.input("src","key1", empty().set("subkey","subkey1"))
            it.input("src","key2", empty().set("subkey","subkey2"))
            val (k1,v1) = it.output("mysink")
            val (k2,v2) = it.output("mysink")
            // TODO continue
        }
    }
    @Test
    fun testSingleToManyJoin() {
        pipe("somegen") {
            source("@left") {
                joinGrouped(optional = true) {
                    source("@right") {
                        group { msg->msg["foreignkey"] as String }
                    }
                }
                set { left, right ->
                    left["rightsub"] = right["list"]
                    left
                }
                each{ left,right,key->
                        logger.info( "Message: ${left} + $right + $key")
                }
                sink("@output")
            }
        }.renderAndTest {
            assertTrue(it.isEmpty("@output"))
            it.input("@left", "key1", empty().set("name", "left1"))
            assertTrue(!it.isEmpty("@output"))
            val (key,value) = it.output("@output")
            assertEquals("key1",key)
            val sublist: List<IMessage> = (value["rightsub"]?: emptyList<IMessage>()) as List<IMessage>
            assertTrue (sublist.isEmpty())
//            it.input()
        }
    }
}
