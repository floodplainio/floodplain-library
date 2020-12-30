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
@file:Suppress("UNCHECKED_CAST")

package io.floodplain.kotlindsl.message

import io.floodplain.immutable.api.ImmutableMessage
import io.floodplain.immutable.factory.ImmutableFactory
import java.math.BigDecimal
import java.util.Date
import kotlin.streams.toList

interface IMessage {
    operator fun get(path: String): Any?
    fun message(path: String): IMessage
    fun string(path: String): String
    fun integer(path: String): Int
    fun optionalInteger(path: String): Int?

    fun date(path: String): Date
    fun optionalDate(path: String): Date?
    fun long(path: String): Long
    fun optionalLong(path: String): Long?
    fun list(path: String): List<String>
    fun optionalList(path: String): List<String>?
    fun decimal(path: String): BigDecimal
    fun optionalDecimal(path: String): BigDecimal?
    fun double(path: String): Double
    fun optionalDouble(path: String): Double?
    fun boolean(path: String): Boolean
    fun optionalBoolean(path: String): Boolean?
    fun optionalString(path: String): String?
    fun clear(path: String): IMessage
    fun clearAll(paths: List<String>): IMessage
    fun isEmpty(): Boolean
    operator fun set(path: String, value: Any?): IMessage
    fun copy(): IMessage
    fun toImmutable(): ImmutableMessage
    fun data(): Map<String, Any>
    fun merge(msg: IMessage): IMessage
    fun messageList(path: String): List<IMessage>?
}

private data class IMessageImpl(private val content: MutableMap<String, Any>) : IMessage {

    //    val content = mapOf<String,Any>()
    override operator fun get(path: String): Any? {
        val (msg, name) = parsePath(path.split("/"))
        return msg.content.get(name)
    }

    override fun message(path: String): IMessage {
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
    override fun string(path: String): String {
        return optionalString(path)
            ?: throw NullPointerException("Can't obtain string from path: $path as it is absent")
    }

    override fun integer(path: String): Int {
        return optionalInteger(path) ?: throw NullPointerException("Can't obtain int from path: $path as it is absent")
    }

    override fun optionalInteger(path: String): Int? {
        val raw = get(path) ?: return null
        if (raw !is Int) {
            throw ClassCastException("Path element $path should be an integer but it is a ${raw::class}")
        }
        return raw
    }
    override fun long(path: String): Long {
        return optionalLong(path) ?: throw NullPointerException("Can't obtain long from path: $path as it is absent")
    }

    override fun optionalLong(path: String): Long? {
        val raw = get(path) ?: return null
        if (raw !is Long) {
            throw ClassCastException("Path element $path should be an long but it is a ${raw::class}")
        }
        return raw
    }

    override fun date(path: String): Date {
        return optionalDate(path) ?: throw NullPointerException("Can't obtain date from path: $path as it is absent")
    }

    override fun optionalDate(path: String): Date? {
        val raw = get(path) ?: return null
        if (raw !is Date) {
            throw ClassCastException("Path element $path should be an date but it is a ${raw::class}")
        }
        return raw
    }

    override fun list(path: String): List<String> {
        return optionalList(path) ?: throw NullPointerException("Can't obtain list from path: $path as it is absent")
    }

    override fun optionalList(path: String): List<String>? {
        val raw = get(path) ?: return null
        if (raw !is List<*>) {
            throw ClassCastException("Path element $path should be an list but it is a ${raw::class}")
        }
        return raw as List<String>
    }

    override fun decimal(path: String): BigDecimal {
        return optionalDecimal(path) ?: throw NullPointerException("Can't obtain decimal from path: $path as it is absent")
    }

    override fun optionalDecimal(path: String): BigDecimal? {
        val raw = get(path) ?: return null
        if (raw !is BigDecimal) {
            throw ClassCastException("Path element $path should be an decimal but it is a ${raw::class}")
        }
        return raw
    }

    override fun double(path: String): Double {
        return optionalDouble(path) ?: throw NullPointerException("Can't obtain double from path: $path as it is absent")
    }

    override fun optionalDouble(path: String): Double? {
        val raw = get(path) ?: return null
        if (raw !is Double) {
            throw ClassCastException("Path element $path should be an double but it is a ${raw::class}")
        }
        return raw
    }

    override fun messageList(path: String): List<IMessage>? {
        val raw = get(path) ?: return null
        if (raw !is List<*>) {
            throw ClassCastException("Path element $path should be an double but it is a ${raw::class}")
        }
        return raw as List<IMessage>
    }

    override fun boolean(path: String): Boolean {
        return optionalBoolean(path) ?: throw NullPointerException("Can't obtain boolean from path: $path as it is absent")
    }

    override fun optionalBoolean(path: String): Boolean? {
        val raw = get(path) ?: return null
        if (raw !is Boolean) {
            throw ClassCastException("Path element $path should be an boolean but it is a ${raw::class}")
        }
        return raw
    }

    override fun optionalString(path: String): String? {
        val raw = get(path) ?: return null
        if (raw !is String) {
            throw ClassCastException("Path element $path should be an string but it is a ${raw::class}")
        }
        return raw
    }

    private fun parsePath(path: List<String>): Pair<IMessageImpl, String> {
        if (path.size > 1) {
            val subElement: Any? = content[path[0]]
            subElement ?: throw NullPointerException("Can't parse path list: $path, element ${path[0]} is missing")
            if (subElement !is IMessageImpl) {
                throw ClassCastException("Path element ${path[0]} should be a message but it is a ${subElement::class}")
            }
            return subElement.parsePath(path.subList(1, path.size))
        }
        return Pair(this, path[0])
    }

    override fun clear(path: String): IMessage {
        val (msg, name) = parsePath(path.split("/"))
        msg.content.remove(name)
        return msg
    }

    override fun clearAll(paths: List<String>): IMessage {
        paths.forEach { clear(it) }
        return this
    }

    override fun isEmpty(): Boolean {
        return content.isEmpty()
    }

    override operator fun set(path: String, value: Any?): IMessage {
        val (msg, name) = parsePath(path.split("/"))
        if (value == null) {
            msg.content.remove(name)
        } else {
            if (value is IMessage) {
                if (!value.isEmpty()) {
                    msg.content[name] = value
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

    override fun copy(): IMessage {
        return fromData(content)
        // IMessage(data().toMutableMap())
    }

    private fun fromData(data: Map<String, Any>): IMessage {
        val convertedMap = data.map { (key, value) ->
            when (value) {
                is Map<*, *> -> key to fromData(value as Map<String, Any>)
                is List<*> -> key to parseList(value)
                else -> key to value
            }
        }.toMap()
        return IMessageImpl(convertedMap.toMutableMap())
    }

    private fun parseList(input: List<*>): List<*> {
        if (input.isEmpty()) {
            // Decide on this scenario
            return emptyList<IMessage>()
        }
        val first = input.first()!!
        if (first is Map<*, *>) {
            return input.map { fromData(it as Map<String, Any>) }.toList()
        }
        return input
    }

    override fun toImmutable(): ImmutableMessage {
        val values = mutableMapOf<String, Any>()
        val types = mutableMapOf<String, ImmutableMessage.ValueType>()
        val subMessage = mutableMapOf<String, ImmutableMessage>()
        val subMessageList = mutableMapOf<String, List<ImmutableMessage>>()
        for ((name, elt) in content) {
            when (elt) {
                is IMessage -> subMessage[name] = elt.toImmutable()
                is List<*> -> maybeSetList(name, elt, subMessageList, values, types)
                is ImmutableMessage -> subMessage[name] = elt
                else -> {
                    values[name] = elt; types[name] = ImmutableFactory.resolveTypeFromValue(elt)
                }
            }
        }
        return ImmutableFactory.create(values, types, subMessage, subMessageList)
    }
    private fun maybeSetList(name: String, value: List<*>, subMessagesList: MutableMap<String, List<ImmutableMessage>>, values: MutableMap<String, Any>, types: MutableMap<String, ImmutableMessage.ValueType>) {
        if (value.isEmpty()) {
            return
        }
        val first = value.first() ?: throw java.lang.NullPointerException("Sub list contains a null")
        if (first is IMessage) {
            subMessagesList[name] = value.map {
                (it as IMessageImpl).toImmutable()
            }.toList()
        } else {
            values[name] = value as List<String>
            types[name] = ImmutableMessage.ValueType.STRINGLIST
        }
    }

    // private fun subListToImmutable(items: List<*>): List<ImmutableMessage> {
    //     if(items.isEmpty()) {
    //         return emptyList()
    //     }
    //     val first = items.first() ?: throw java.lang.NullPointerException("Sub list contains a null")
    //     if(first is IMessage) {
    //         return items.stream().map {
    //             (it as IMessage).toImmutable()
    //         }.collect(Collectors.toList())
    //     }
    // }

    override fun toString(): String {
        return this.content.toString()
    }

    override fun data(): Map<String, Any> {
        return content.map { (key, value) ->
            key to when (value) {
                is IMessage -> value.data()
                is List<*> -> subMessageListIfSM(value)
                else -> value
            }
        }.toMap()
    }

    override fun merge(msg: IMessage): IMessage {
        msg.data().forEach { (k, v) ->
            set(k, v)
        }
        return this
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IMessageImpl) return false

        if (content != other.content) return false

        return true
    }

    override fun hashCode(): Int {
        return content.hashCode()
    }
}

private fun subMessageListIfSM(value: List<*>): List<*> {
    if (value.isEmpty()) {
        return listOf<String>()
    }
    val first = value.first()!!
    if (first is IMessage) {
        return value.map { (it as IMessage).data() }.toList()
    }
    // Assume list of strings
    return value
}

fun empty(): IMessage = IMessageImpl(mutableMapOf())

fun fromImmutable(msg: ImmutableMessage): IMessage {
    val content = mutableMapOf<String, Any>()
    for ((name, value: Any?) in msg.values()) {
        if (value != null) {
            content[name] = value
        }
    }
    for ((name, value) in msg.subMessageMap()) {
        content[name] = fromImmutable(value)
    }
    for ((name, value) in msg.subMessageListMap()) {
        content[name] = value.stream().map { e -> fromImmutable(e) }.toList()
    }
    return IMessageImpl(content)
}
