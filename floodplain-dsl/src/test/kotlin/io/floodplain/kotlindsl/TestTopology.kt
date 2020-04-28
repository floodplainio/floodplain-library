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
            val record1 = empty().set("subkey", "subkey1")
            val record2 = empty().set("subkey", "subkey2")
            it.input("src","key1", record1)
            it.input("src","key2", record2)
            val (k1,v1) = it.output("mysink")
            assertEquals("subkey1|key1",k1)
            assertEquals(record1,v1)
            val (k2,v2) = it.output("mysink")
            assertEquals("subkey2|key2",k2)
            assertEquals(record2,v2)

            // TODO continue
        }
    }
    @Test
    fun testSingleToManyJoinOptional() {
        pipe("somegen") {
            source("@left") {
                joinGrouped(optional = true,debug = true) {
                    source("@right") {
                        group { msg->msg["foreignkey"] as String }
                    }
                }
                set { left, right ->
                    left["rightsub"] = right["list"]
                    left
                }
                each{ left,right,key->
                        logger.info( "Message: ${left} RightMessage $right key: $key")
                }
                sink("@output")
            }
        }.renderAndTest {
            assertTrue(it.isEmpty("@output"))
            val leftRecord = empty().set("name", "left1")
            it.input("@left", "key1", leftRecord)
            assertTrue(!it.isEmpty("@output"))
            val record1 = empty().set("foreignkey", "key1").set("recorddata","data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata","data2")
            it.input("@right","otherkey1", record1)
            it.input("@right","otherkey2", record2)

            val (key,value) = it.output("@output")
            assertEquals("key1",key)
            val sublist: List<IMessage> = (value["rightsub"]?: emptyList<IMessage>()) as List<IMessage>
            assertTrue (sublist.isEmpty())

            val outputs = it.outputSize("@output")
            assertEquals(2,outputs,"should have 2 elements")
            it.output("@output") // skip one
            val (_,v3) = it.output("@output")
            val subList = v3.get("rightsub") as List<*>
            assertEquals(2,subList.size)
            assertEquals(record1,subList[0])
            assertEquals(record2,subList[1])
            it.delete("@right","otherkey1")
            val (_,v4) = it.output("@output")
            val subList2 = v4.get("rightsub") as List<*>
            assertEquals(1,subList2.size)
            it.delete("@right","otherkey2")
            val (_,v5) = it.output("@output")
            val subList3 = v5.get("rightsub") as List<*>
            assertEquals(0,subList3.size)
            it.delete("@left","key1")
            assertEquals("key1",it.deleted("@output"))
        }
    }
    @Test
    fun testSingleToManyJoin() {
        pipe("somegen") {
            source("@left") {
                joinGrouped(debug = true) {
                    source("@right") {
                        group { msg->msg["foreignkey"] as String }
                    }
                }
                set { left, right ->
                    left["rightsub"] = right["list"]
                    left
                }
                each{ left,right,key->
                    logger.info( "Message: ${left} RightMessage $right key: $key")
                }
                sink("@output")
            }
        }.renderAndTest {
            assertTrue(it.isEmpty("@output"))
            val leftRecord = empty().set("name", "left1")
            it.input("@left", "key1", leftRecord)
//            assertTrue(!it.isEmpty("@output"))

            val record1 = empty().set("foreignkey", "key1").set("recorddata","data1")
            val record2 = empty().set("foreignkey", "key1").set("recorddata","data2")
            it.input("@right","otherkey1", record1)
            it.input("@right","otherkey2", record2)

            // TODO skip the 'ghost delete' I'm not too fond of this, this one should be skippable
            it.deleted("@output")
            val outputs = it.outputSize("@output")
            assertEquals(2,outputs,"should have 2 elements")
            it.output("@output") // skip one
            val (_,v3) = it.output("@output")
            val subList = v3.get("rightsub") as List<*>
            assertEquals(2,subList.size)
            assertEquals(record1,subList[0])
            assertEquals(record2,subList[1])
            it.delete("@right","otherkey1")
            val (_,v4) = it.output("@output")
            val subList2 = v4.get("rightsub") as List<*>
            assertEquals(1,subList2.size)
            it.delete("@right","otherkey2")
            assertEquals("key1",it.deleted("@output"))
        }
    }

}
