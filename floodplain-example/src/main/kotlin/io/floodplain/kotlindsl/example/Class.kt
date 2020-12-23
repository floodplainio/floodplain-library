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
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.group
import io.floodplain.kotlindsl.join
import io.floodplain.kotlindsl.joinGrouped
import io.floodplain.kotlindsl.joinRemote
import io.floodplain.kotlindsl.set
import io.floodplain.kotlindsl.sink

import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.streams

private val logger = mu.KotlinLogging.logger {}


fun main() {

    val generation = "generation16"
    val deployment = "develop"
    val tenant = "KNBSB"
    // val deployment = "develop"
    streams(tenant,deployment, generation) {
        listOf(
            source("sportlinkkernel-ADDRESS") {
            set { _, msg, _->
                // TODO formatzipcode
                msg["zipcode"] = msg.optionalString("zipcode")?:"".replace(" ","").trim()
                msg.clear("updateby")
                msg.clear("lastupdate")
                msg
            }
            filter { key, msg ->
                msg["zipcode"]!=null
            }
            // group { msg->
            //     logger.info("key: $key\n>>>totalmessage: $msg")
            //     msg.string("zipcode")
            // }
            joinRemote({msg->msg.string("zipcode")},false) {
                source("sportlinkkernel-ZIPCODEPOSITION") {
                    set { _, msg, _->
                        msg.clear("updateby")
                        msg.clear("lastupdate")
                        msg["point"] = "${msg["longitude"]},${msg["latitude"]}"
                        msg
                    }
                }
            }
            set { key, address, position ->
                address["position"] = position
                // logger.info("Key: $key")
                // logger.info("address: $address")
                // logger.info("position: $position")
                address
            }
            each { key,msg,_->
                logger.info("Key: $key")
                logger.info("Msg: ${msg}")
            }
            joinRemote({msg->msg.string("zipcode")},false) {
                source("sportlinkkernel-POSTCODETABLE") {
                    set { _, msg, _ ->
                        msg.clearAll(listOf("updateby","lastupdate","unipostcodereeksindicatie","huisnrtot","straatnaam","huisnrvan","woonplaats"))
                        msg
                    }
                    group{ msg->msg["communityid"] as String }
                    // each { key,msg,_->
                    //     logger.info("PostcodeTableKey: $key")
                    //     logger.info("PostcodeTableMsg: $msg")
                    // }
                    joinRemote({msg->msg.string("communityid")},false) {
                        source("sportlinkkernel-COMMUNITY") {
                            set { _, msg, _ ->
                                msg["communityname"] = msg["name"]
                                msg.clear("name")
                                msg
                            }
                        }
                    }
                    set { _, postcode, community ->
                        postcode["communityname"] = community["communityname"]
                        postcode
                    }
                    // sink("@POSTCODE")
                }
            }
            // set { _, address, position ->
            //     address["position"] = position
            //     address
            //
            // }
            sink("@addresswithposition")
        },
        source("sportlinkkernel-CLASS") {
            joinGrouped (true,false) {
                source("sportlinkkernel-CLASSATTRIBUTE") {
                    group {  }
                }
            }
        }
            )
    }.renderAndSchedule(null, "10.8.0.7:9092")
    logger.info { "done!" }
}

// <store topic="sportlinkkernel-ZIPCODEPOSITION">
// <remove name="updateby"/>
// <remove name="lastupdate"/>
// <createcoordinate from="longitude,latitude" to="point"/>
// </store>

