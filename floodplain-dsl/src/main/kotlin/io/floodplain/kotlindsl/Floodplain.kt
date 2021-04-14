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

import io.floodplain.immutable.api.ImmutableMessage
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.kotlindsl.transformer.BufferTransformer
import io.floodplain.kotlindsl.transformer.DiffTransformer
import io.floodplain.kotlindsl.transformer.ForkTransformer
import io.floodplain.reactive.source.topology.DynamicSinkTransformer
import io.floodplain.reactive.source.topology.EachTransformer
import io.floodplain.reactive.source.topology.FilterTransformer
import io.floodplain.reactive.source.topology.GroupTransformer
import io.floodplain.reactive.source.topology.JoinRemoteTransformer
import io.floodplain.reactive.source.topology.JoinWithTransformer
import io.floodplain.reactive.source.topology.KeyTransformer
import io.floodplain.reactive.source.topology.ScanTransformer
import io.floodplain.reactive.source.topology.SetTransformer
import io.floodplain.reactive.source.topology.SinkTransformer
import io.floodplain.reactive.source.topology.TopicSource
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.reactive.topology.ReactivePipe
import io.floodplain.replication.api.ReplicationMessage
import io.floodplain.streams.api.Topic
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.connect.sink.SinkTask
import java.time.Duration
import java.util.Optional

/**
 * Super (wrapper) class for all components (source, transformer or sink)
 */
open class Transformer(rootTopology: Stream, val component: TopologyPipeComponent, override val topologyContext: TopologyContext) : PartialStream(rootTopology) {
    // override fun addSource(source: Source) {
    //     rootTopology.addSource(source)
    // }
}

/**
 * Base class for connector configurations
 */
interface Config {

    val topologyContext: TopologyContext
    val topologyConstructor: TopologyConstructor

    /**
     * Connector configs must implement this method, returning a name + map.
     * The map is essentially a Kafka Connect configuration, and will be converted to JSON and posted to Kafka Connect
     * For some
     */
    fun materializeConnectorConfig(): List<MaterializedConfig>
}

interface SourceConfig : Config {
    fun sourceElements(): List<SourceTopic>
    suspend fun connectSource(inputReceiver: InputReceiver)
}

interface SinkConfig : Config {
    fun sinkTask(): Any?
    fun instantiateSinkElements(): List<Map<String, String>>
    fun sinkElements(): Map<Topic, List<FloodplainSink>>
}

abstract class AbstractSinkConfig : SinkConfig {
    // var instantiatedSinkElements: Map<Topic, List<FloodplainSink>>? = null
    var sinkTask: SinkTask? = null
    var floodplainSink: FloodplainSink? = null
    var connector: SinkConnector? = null
    override fun instantiateSinkElements(): List<Map<String, String>> {
        val configs = materializeConnectorConfig()
        if (configs.size > 1) {
            throw RuntimeException("Multiple configs not supported for now")
        }
        return configs.map {
            it.settings
        }.toList()
    }
    override fun sinkElements(): Map<Topic, List<FloodplainSink>> {
        return instantiateSinkConfig(this)
    }
}

class MaterializedConfig(val name: String, val topics: List<Topic>, val settings: Map<String, String>)

interface SourceTopic {
    fun topic(): Topic
    fun schema(): String?
    fun table(): String
}

interface FloodplainSink {
    fun send(topic: Topic, elements: List<Pair<ByteArray?, ByteArray?>>)
    fun config(): SinkConfig
    fun flush()
    fun close()
    fun taskObject(): Any?
}

/**
 * Filter a source. The supplied lambda should return 'true' if the message should be propagated, 'false' if not.
 */
fun PartialStream.filter(flt: (String, IMessage) -> Boolean) {
    val transformerFilter: (String, ImmutableMessage) ->
    Boolean = { key, msg: ImmutableMessage -> flt.invoke(key, fromImmutable(msg)) }
    val transformer = FilterTransformer(transformerFilter)
    addTransformer(Transformer(rootTopology, transformer, topologyContext))
}

/**
 * Perform the supplied lambda for each message. You can use this for logging or other side effects,
 * this function should not influence the semantics of the stream
 */
fun PartialStream.each(transform: (String, IMessage, IMessage) -> Unit): Transformer {
    val transformer: (String, ImmutableMessage, ImmutableMessage) -> Unit =
        { key: String, msg: ImmutableMessage, param: ImmutableMessage ->
            transform.invoke(key, fromImmutable(msg), fromImmutable(param))
        }
    val each = EachTransformer(transformer)
    return addTransformer(Transformer(this.rootTopology, each, topologyContext))
}

/**
 * Only propagates if a message is actually different than the previous message with the same key
 */
fun PartialStream.diff(): Transformer {
    val diffTransformer = DiffTransformer()
    return addTransformer(Transformer(this.rootTopology, diffTransformer, topologyContext))
}

/**
 * Buffers messages until either: duration has past, or maxSize number of messages have been accumulated since the last
 * flush.
 * inMemory defines if it is a volitile buffer
 */
fun PartialStream.buffer(duration: Duration, maxSize: Int = 10000, inMemory: Boolean = false): Transformer {
    val bufferTransformer = BufferTransformer(duration, maxSize, inMemory)
    return addTransformer(Transformer(this.rootTopology, bufferTransformer, topologyContext))
}

/**
 * Modifies the incoming message before passing it on.
 * You can use it as a simple stateless transformation: Add constant values, remove fields or transform fields.
 * A 'set' is also required for more complicated combinations. Typically after a join you use a 'set' to merge the messages
 * from both sources.
 * The first message is the original message from the join, the second message the result from the 'secondary' source.
 * The second message will be lost after this operation, so you need to append anything you want to keep from the inner
 * source to the outer source here.
 * Alternatively, you can create an all-new message and just add the things you are interested in, and return that.
 */
fun PartialStream.set(transform: (String, IMessage, IMessage) -> IMessage): Transformer {
    val transformer: (String, ImmutableMessage, ImmutableMessage) -> ImmutableMessage =
        { key, msg, param ->
            transform.invoke(key, fromImmutable(msg), fromImmutable(param)).toImmutable()
        }
    val set = SetTransformer(transformer)
    return addTransformer(Transformer(this.rootTopology, set, topologyContext))
}

fun PartialStream.only(vararg fields: String): Transformer {
    val transformer: (String, ImmutableMessage, ImmutableMessage) -> ImmutableMessage =
        { _, msg, _  ->
            val result = empty()
            val input = fromImmutable(msg)
            fields.forEach {
                result[it] = input[it]
            }
            result.toImmutable()
        }
    val set = SetTransformer(transformer)
    return addTransformer(Transformer(this.rootTopology, set, topologyContext))
}


/**
 * Modifies the incoming message before passing it on.
 * It is a simpler sibling to set, as there is no access to the key or the secondary message
 */
fun PartialStream.transform(transform: (IMessage) -> IMessage): Transformer {
    val transformer: (String, ImmutableMessage, ImmutableMessage) -> ImmutableMessage =
        { _, msg, _ ->
            transform.invoke(fromImmutable(msg)).toImmutable()
        }
    val set = SetTransformer(transformer)
    return addTransformer(Transformer(this.rootTopology, set, topologyContext))
}

fun PartialStream.keyTransform(transform: (String) -> String): Transformer {
    return addTransformer(Transformer(this.rootTopology, KeyTransformer(transform), topologyContext))
}

// fun PartialPipe.copy()
/**
 * Join the current source with another source, that has a different key.
 * @param key This lambda should extract the key we want to use from the current source.
 * Keys are always strings in floodplain.
 * So it takes a message from the source, and returns the (string) key to join with
 * @param optional If true, this transformation will also emit a result when there is no matching result.
 * Consider that this might result in more emissions, so that could carry a performance penalty
 * @param source The source to join with
 *
 */
fun PartialStream.joinRemote(key: (IMessage) -> String, optional: Boolean = false, source: () -> Source) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val jrt = JoinRemoteTransformer(source.invoke().toReactivePipe(), keyExtractor, optional, false)
    addTransformer(Transformer(this.rootTopology, jrt, topologyContext))
}

fun PartialStream.joinRemote(vararg keys: String, optional: Boolean = false, source: () -> Source) {
    joinRemote(
        { msg ->
            keys.joinToString(ReplicationMessage.KEYSEPARATOR) { msg[it].toString() }
        },
        optional,
        source
    )
}

fun PartialStream.joinMulti(key: (IMessage) -> String?, secondaryKey: (IMessage) -> String, optional: Boolean = false, source: () -> Source) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String? = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val secondarySource = source()
    secondarySource.group(secondaryKey)

    val jrt = JoinRemoteTransformer(secondarySource.toReactivePipe(), keyExtractor, optional, true)
    addTransformer(Transformer(this.rootTopology, jrt, topologyContext))
}


fun PartialStream.joinAttributes(withTopic: String, nameAttribute: String, valueAttribute: String, vararg keys: String) {
    return joinAttributes(withTopic, nameAttribute, valueAttribute) { msg ->
        keys.joinToString(ReplicationMessage.KEYSEPARATOR) { msg[it].toString() }
    }
}

/**
 * C/p from joinAttributes, but qualified
 */
fun PartialStream.joinAttributes(withTopic: String, nameAttribute: String, valueAttribute: String, keyExtract: (IMessage) -> String) {
    join(optional = true, debug = false) {
        from(withTopic) {
            scan(
                keyExtract,
                {
                    empty()
                },
                {
                    set { _, msg, acc ->
                        val name = msg[nameAttribute] as String?
                        acc[name!!] = msg[valueAttribute]
                        acc
                    }
                },
                {
                    set { _, msg, acc ->
                        acc.clearAll(msg[nameAttribute] as String)
                        acc
                    }
                }
            )
        }
    }
}
/**
 * Group a source, using the key from the lambda. The messages will be unchanged,
 * only the key will have the supplied key pre-pended.
 */
fun PartialStream.group(key: (IMessage) -> String) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val group = GroupTransformer(keyExtractor)
    addTransformer(Transformer(this.rootTopology, group, topologyContext))
}

/**
* Create sub source (without qualifying with tenant / deployment / gen)
*/
fun PartialStream.from(topic: String, init: Source.() -> Unit = {}): Source {
    return createSource(Topic.fromQualified(topic, rootTopology.topologyContext), rootTopology, init)
}

/**
 * Create toplevel source (fully qualified)
 */
fun Stream.from(topic: String, init: Source.() -> Unit = {}) {
    addSource(createSource(Topic.fromQualified(topic, rootTopology.topologyContext), this, init))
}

private fun createSource(
    topic: Topic,
    rootTopology: Stream,
    init: Source.() -> Unit
): Source {
    val sourceElement = TopicSource(
        topic,
        Topic.FloodplainKeyFormat.FLOODPLAIN_STRING,
        Topic.FloodplainBodyFormat.FLOODPLAIN_JSON
    )
    val source = Source(rootTopology, sourceElement, rootTopology.topologyContext)
    source.init()
    return source
}

/**
 * Source with a 'custom' format, the topic should be fully qualified TODO directly add key/value serde
 */
fun Stream.externalSource(
    topic: String,
    keyFormat: Topic.FloodplainKeyFormat,
    valueFormat: Topic.FloodplainBodyFormat,
    init: Source.() -> Unit
) {
    val sourceElement = TopicSource(Topic.fromQualified(topic, this.topologyContext), keyFormat, valueFormat)
    val source = Source(this, sourceElement, this.topologyContext)
    source.init()
    rootTopology.addSource(source)
}

/**
 * Add qualified source in Kafka, used.
 */
fun Stream.debeziumSource(topicSource: String, init: Source.() -> Unit = {}) {
    rootTopology.addSource(existingDebeziumSource(topicSource, this, init))
}

fun FloodplainOperator.debeziumSource(topicSource: String, init: Source.() -> Unit = {}): Source {
    return existingDebeziumSource(topicSource, this.rootTopology, init)
}

private fun existingDebeziumSource(topicSource: String, rootTopology: Stream, init: Source.() -> Unit = {}): Source {
    val topic = Topic.fromQualified(topicSource, rootTopology.topologyContext)
    val sourceElement = TopicSource(topic, Topic.FloodplainKeyFormat.CONNECT_KEY_JSON, Topic.FloodplainBodyFormat.CONNECT_JSON)
    val source = Source(rootTopology, sourceElement, rootTopology.topologyContext)
    source.init()
    return source
}

/**
 * Creates a simple sink that will contain the result of the current transformation. Will not qualify with tenant / deployment
 */
fun PartialStream.to(topic: String): Transformer {
    val sink = SinkTransformer(
        Optional.empty(),
        Topic.fromQualified(topic, topologyContext),
        Optional.empty(),
        Topic.FloodplainKeyFormat.FLOODPLAIN_STRING,
        Topic.FloodplainBodyFormat.FLOODPLAIN_JSON
    )
    return addTransformer(Transformer(this.rootTopology, sink, topologyContext))
}


/**
 * Creates a simple sink that will contain the result of the current transformation.
 */
fun PartialStream.toExternal(topic: String): Transformer {
    val sink = SinkTransformer(
        Optional.empty(),
        Topic.fromQualified(topic, topologyContext),
        Optional.empty(),
        Topic.FloodplainKeyFormat.CONNECT_KEY_JSON,
        Topic.FloodplainBodyFormat.CONNECT_JSON
    )
    return addTransformer(Transformer(this.rootTopology, sink, topologyContext))
}

/**
 * Creates a simple sink that takes a lambda that will choose a **qualified** topic */
fun PartialStream.dynamicSink(name: String, extractor: (String, IMessage) -> String): Transformer {
    val sink =
        DynamicSinkTransformer(name, Optional.empty()) { key, value -> extractor.invoke(key, fromImmutable(value)) }
    return addTransformer(Transformer(rootTopology, sink, topologyContext))
}

/**
 * Joins with another source using the same key as this source.
 * @param optional: If set to true, it will also emit a value if there is no counterpart (yet) in the supplied source.
 * Note that setting this to true can cause a performance penalty, as more items could be emitted.
 */
fun PartialStream.join(optional: Boolean = false, debug: Boolean = false, source: PartialStream.() -> Source) {
    val jrt = JoinWithTransformer(optional, false, source.invoke(this).toReactivePipe(), debug)
    addTransformer(Transformer(rootTopology, jrt, topologyContext))
}

/**
 * Joins with another source that has been grouped with the key of the current source.
 * This generally means that the inner source should end with a 'group' transformation
 * @param optional: If set to true, it will also emit a value if there is no counterpart (yet) in the supplied source.
 * Note that setting this to true can cause a performance penalty, as more items could be emitted.
 */
fun PartialStream.joinGrouped(
    optional: Boolean = false,
    debug: Boolean = false,
    source: () -> Source
) {
    val jrt = JoinWithTransformer(optional, true, source.invoke().toReactivePipe(), debug)
    addTransformer(Transformer(rootTopology, jrt, topologyContext))
}

/**
 * Scan is effectively a 'reduce' operator (The 'scan' name is used in Rx, which means a reduce operator that emits a
 * 'running aggregate' every time it consumes a message). A real reduce makes no sense in infinite streams (as it would
 * emit a single item when the stream completes, which never happens).
 * It is a little bit more complicated than a regular reduce operator, as it also allows deleting items
 * @param key This lambda extracts the aggregate key from the message. In SQL terminology, it is the 'group by'.
 * Scan will create a aggregation bucket for each distinct key
 * @param initial Create the initial value for the aggregator
 */
fun PartialStream.scan(
    key: (IMessage) -> String,
    initial: (IMessage) -> IMessage,
    onAdd: Block.() -> Transformer,
    onRemove: Block.() -> Transformer
) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val initialConstructor: (ImmutableMessage) -> ImmutableMessage =
        { msg -> initial.invoke(fromImmutable(msg)).toImmutable() }
    val onAddBlock = createBlock()
    onAdd.invoke(onAddBlock)
    val onRemoveBlock = createBlock()
    onRemove.invoke(onRemoveBlock)
    addTransformer(
        Transformer(
            rootTopology,
            ScanTransformer(
                keyExtractor,
                initialConstructor,
                onAddBlock.transformers.map { e -> e.component },
                onRemoveBlock.transformers.map { e -> e.component }
            ),
            topologyContext
        )
    )
}

fun PartialStream.createBlock(): Block {
    return Block(rootTopology, topologyContext)
}
/**
 * Scan is effectively a 'reduce' operator (The 'scan' name is used in Rx, which means a reduce operator that emits a
 * 'running aggregate' every time it consumes a message). A real reduce makes no sense in infinite streams
 * (as it would emit a single item when the stream completes, which never happens).
 * It is a little bit more complicated than a regular reduce operator, as it also allows deleting items
 * This version of scan has no key, it does an aggregate over the entire topic, so it ends up being a single-key topic
 * @param initial Create the initial value for the aggregator
 *
 */
fun PartialStream.scan(
    initial: (IMessage) -> IMessage,
    onAdd: Block.() -> Transformer,
    onRemove: Block.() -> Transformer
): Transformer {
    val initialConstructor: (ImmutableMessage) -> ImmutableMessage =
        { msg -> initial.invoke(fromImmutable(msg)).toImmutable() }
    val onAddBlock = createBlock()
    onAdd.invoke(onAddBlock)
    val onRemoveBlock = createBlock()
    onRemove.invoke(onRemoveBlock)
    return addTransformer(
        Transformer(
            rootTopology,
            ScanTransformer(
                null,
                initialConstructor,
                onAddBlock.transformers.map { e -> e.component },
                onRemoveBlock.transformers.map { e -> e.component }
            ),
            topologyContext
        )
    )
}

/**
 * Fork the current stream to other downstreams
 */
fun PartialStream.fork(vararg destinations: Block.() -> Unit): Transformer {
    val blocks = destinations.map { dd -> val b = Block(rootTopology, topologyContext); dd(b); b }.toList()
    val forkTransformer = ForkTransformer(blocks)
    return addTransformer(Transformer(rootTopology, forkTransformer, topologyContext))
}

/**
 * Create a new top level stream instance
 * @param generation This is a string that indicates this current run. If you stop this run and restart it, it will
 * continue where it left off. This might take some getting used to. Usually, when you are developing, you change your
 * code, and restart. In the case of a data transformation like floodplain, it might mean that nothing will happen:
 * All data has already been transformed. To make sure all data gets reprocessed, change the generation. It is a string,
 * with no further meaning within the framework, you can choose what meaning you want to attach. You can increment a
 * number, use a sort of time stamp, or even a git commit.
 */
fun stream(tenant: String? = null, deployment: String? = null, generation: String = "any", init: Stream.() -> Unit): Stream {
    val topologyContext = TopologyContext.context(Optional.ofNullable(tenant), Optional.ofNullable(deployment), generation)
    val pipe = Stream(topologyContext, TopologyConstructor())
    pipe.init()
    return pipe
}

/**
 * Sources wrap a TopologyPipeComponent
 */
// TODO topologycontext could be removed, it can be obtained from the rootTopology
class Source(override val rootTopology: Stream, private val component: TopologyPipeComponent, override val topologyContext: TopologyContext) : PartialStream(rootTopology) {
    fun toReactivePipe(): ReactivePipe {
        return ReactivePipe(component, transformers.map { e -> e.component })
    }
}

/**
 * Common superclass for sources or 'pipe segments', either a source, transformer or block
 */
open class PartialStream(override val rootTopology: Stream) : FloodplainOperator {
    val transformers: MutableList<Transformer> = mutableListOf()

    // override val tenant = topologyContext.tenant
    fun addTransformer(transformer: Transformer): Transformer {
        transformers.add(transformer)
        return transformer
    }

    override val topologyContext: TopologyContext
        get() = rootTopology.topologyContext
    override val tenant: String?
        get() = rootTopology.topologyContext.tenant.orElse(null)
    override val deployment: String
        get() = rootTopology.topologyContext.deployment.orElse(null)
    override val generation: String
        get() = rootTopology.topologyContext.generation
}

interface FloodplainSourceContainer : FloodplainOperator {
    fun addSource(source: Source)
}

interface FloodplainOperator {
    val topologyContext: TopologyContext
    val rootTopology: Stream

    val tenant: String?
    val deployment: String?
    val generation: String
}

/**
 * Concrete version of a partial pipe
 */
class Block(rootTopology: Stream, override val topologyContext: TopologyContext) : PartialStream(rootTopology)

