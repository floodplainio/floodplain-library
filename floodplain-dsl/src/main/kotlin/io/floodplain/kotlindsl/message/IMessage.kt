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
package io.floodplain.kotlindsl.message

import io.floodplain.immutable.api.ImmutableMessage
import io.floodplain.immutable.factory.ImmutableFactory
import java.math.BigDecimal
import java.util.stream.Collectors
import kotlin.streams.toList

private val logger = mu.KotlinLogging.logger {}

data class IMessage(private val content: MutableMap<String, Any>) {

    //    val content = mapOf<String,Any>()
    operator fun get(path: String): Any? {
        val (msg, name) = parsePath(path.split("/"))
        return msg.content.get(name)
    }

    fun message(path: String): IMessage {
        val raw = get(path)
        if (raw == null) {
            val created = empty()
            set(path, created)
            return created
        }
        if (raw !is IMessage) {
            throw ClassCastException("Path element $path should be an message but it is a ${raw::class}")
        }
        return raw
    }
    fun string(path: String): String {
        return optionalString(path)
                ?: throw NullPointerException("Can't obtain string from path: $path as it is absent")
    }

    fun integer(path: String): Int {
        return optionalInteger(path) ?: throw NullPointerException("Can't obtain int from path: $path as it is absent")
    }

    fun optionalInteger(path: String): Int? {
        val raw = get(path) ?: return null
        if (raw !is Int) {
            throw ClassCastException("Path element $path should be an integer but it is a ${raw::class}")
        }
        return raw
    }

    fun decimal(path: String): BigDecimal {
        return optionalDecimal(path) ?: throw NullPointerException("Can't obtain decimal from path: $path as it is absent")
    }

    fun optionalDecimal(path: String): BigDecimal? {
        val raw = get(path) ?: return null
        if (raw !is BigDecimal) {
            throw ClassCastException("Path element $path should be an decimal but it is a ${raw::class}")
        }
        return raw
    }

    fun double(path: String): Double {
        return optionalDouble(path) ?: throw NullPointerException("Can't obtain double from path: $path as it is absent")
    }

    fun optionalDouble(path: String): Double? {
        val raw = get(path) ?: return null
        if (raw !is Double) {
            throw ClassCastException("Path element $path should be an double but it is a ${raw::class}")
        }
        return raw
    }

    fun boolean(path: String): Boolean {
        return optionalBoolean(path) ?: throw NullPointerException("Can't obtain boolean from path: $path as it is absent")
    }

    fun optionalBoolean(path: String): Boolean? {
        val raw = get(path) ?: return null
        if (raw !is Boolean) {
            throw ClassCastException("Path element $path should be an boolean but it is a ${raw::class}")
        }
        return raw
    }

    fun optionalString(path: String): String? {
        val raw = get(path) ?: return null
        if (raw !is String) {
            throw ClassCastException("Path element $path should be an string but it is a ${raw::class}")
        }
        return raw
    }

    private fun parsePath(path: List<String>): Pair<IMessage, String> {
        if (path.size > 1) {
            val subelement: Any? = content[path[0]]
            subelement ?: throw NullPointerException("Can't parse path list: $path, element ${path[0]} is missing")
            if (subelement !is IMessage) {
                throw ClassCastException("Path element ${path[0]} should be a message but it is a ${subelement::class}")
            }
            return subelement.parsePath(path.subList(1, path.size))
        }
        return Pair(this, path[0])
    }

    fun clear(path: String) {
        val (msg, name) = parsePath(path.split("/"))
        msg.content.remove(name)
    }

    fun isEmpty(): Boolean {
        return content.isEmpty()
    }

    operator fun set(path: String, value: Any?): IMessage {
        val (msg, name) = parsePath(path.split("/"))
        if (value == null) {
            msg.content.remove(name)
        } else {
            if (value is IMessage) {
                if (!value.isEmpty()) {
                    msg.content[name] = value
                } else {
                    // empty msg
                }
            } else {
//                if (value is List<*>) {
//                    logger.info { "L: $value" }
//                }
                // regular value
                msg.content[name] = value
            }
        }
        return this
    }

    fun toImmutable(): ImmutableMessage {
        val values = mutableMapOf<String, Any>()
        val types = mutableMapOf<String, ImmutableMessage.ValueType>()
        val subMessage = mutableMapOf<String, ImmutableMessage>()
        val subMessageList = mutableMapOf<String, List<ImmutableMessage>>()
        for ((name, elt) in content) {
            when (elt) {
                is IMessage -> subMessage[name] = elt.toImmutable()
                is List<*> -> subMessageList[name] = subListToImmutable(elt as List<IMessage>)
                is ImmutableMessage -> subMessage[name] = elt
                else -> {
                    values[name] = elt; types.put(name, ImmutableFactory.resolveTypeFromValue(elt))
                }
            }
        }
        return ImmutableFactory.create(values, types, subMessage, subMessageList)
    }

    private fun subListToImmutable(items: List<IMessage>): List<ImmutableMessage> {
        return items.stream().map {
            it.toImmutable()
        }.collect(Collectors.toList())
    }

    override fun toString(): String {
        return this.content.toString()
    }

    fun data(): Map<String, Any> {
        return content.map { (key, value) ->
            key to when (value) {
                is IMessage -> value.data()
                is List<*> -> (value as List<IMessage>).map { it.data() }
                else -> value
            }
        }.toMap()
    }
}

fun empty(): IMessage = IMessage(mutableMapOf())

fun fromImmutable(msg: ImmutableMessage): IMessage {
    val content = mutableMapOf<String, Any>()
    for ((name, value: Any?) in msg.values()) {
        if (value != null) {
            content[name] = value
        }
    }
    for ((name, value) in msg.subMessageMap()) {
        val submsg = fromImmutable(value)
        if (submsg != null) {
            content[name] = submsg
        }
    }
    for ((name, value) in msg.subMessageListMap()) {
        content[name] = value.stream().map { e -> fromImmutable(e) }.toList()
    }
    return IMessage(content)
}
