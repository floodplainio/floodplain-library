@file:Suppress("ImplicitThis")

package io.floodplain.kotlindsl

import io.floodplain.immutable.api.ImmutableMessage
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.fromImmutable
import io.floodplain.reactive.source.topology.*
import io.floodplain.reactive.source.topology.api.TopologyPipeComponent
import io.floodplain.reactive.topology.ReactivePipe
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyConstructor
import java.util.*

private val logger = mu.KotlinLogging.logger {}

open class Transformer(val component: TopologyPipeComponent) : PartialPipe() {
}

/**
 * Base class for connector configurations
 */
abstract class Config() {

    /**
     * Connector configs must implement this method, returning a name + map.
     * The map is essentially a Kafka Connect configuration, and will be converted to JSON and posted to Kafka Connect
     * For some
     */
    abstract fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String, Map<String, String>>

}

/**
 * Filter a source. The supplied lambda should return 'true' if the message should be propagated, 'false' if not.
 */
fun PartialPipe.filter(flt: (IMessage, IMessage) -> Boolean) {
    val transformerFilter: (ImmutableMessage, ImmutableMessage) -> Boolean = { msg: ImmutableMessage, param: ImmutableMessage -> flt.invoke(fromImmutable(msg), fromImmutable(param)) }
    val transformer = FilterTransformer(transformerFilter)
    addTransformer(Transformer(transformer))
}

/**
 * Perform the supplied lambda for each message. You can use this for logging or other side effects, this function should not influence the semantics of the stream
 */
fun PartialPipe.each(transform: (IMessage, IMessage, String) -> Unit): Transformer {
    val transformer: (ImmutableMessage, ImmutableMessage, String) -> Unit = { msg: ImmutableMessage, param: ImmutableMessage, key: String -> transform.invoke(fromImmutable(msg), fromImmutable(param), key) }
    val each = EachTransformer(transformer)
    return addTransformer(Transformer(each))

}

/**
 * Modifies the incoming message before passing it on.
 * You can use it as a simple stateless transformation: Add constant values, remove fields or transform fields.
 * A 'set' is also required for more complicated combinations. Typically after a join you use a 'set' to merge the messages
 * from both sources.
 * The first message is the original message from the join, the second message the result from the 'inner' source.
 * The second message will be lost after this operation, so you need to append anything you want to keep from the inner source to the outer source here.
 * Alternatively, you can create an all-new message and just add the things you are interested in, and return that.
 */
fun PartialPipe.set(transform: (IMessage, IMessage) -> IMessage): Transformer {
    val transformer: (ImmutableMessage, ImmutableMessage) -> ImmutableMessage = {
        msg: ImmutableMessage, param: ImmutableMessage -> transform.invoke(fromImmutable(msg), fromImmutable(param)).toImmutable()
    }
    val set = SetTransformer(transformer)
    return addTransformer(Transformer(set))
}

//fun PartialPipe.copy()
/**
 * Join the current source with another source, that has a different key.
 * @param key This lambda should extract the key we want to use from the current source. Keys are always strings in floodplain
 * So it takes a message from the source, and returns the (string) key to join with
 * @param optional If true, this transformation will also emit a result when there is no matching result. Consider that this might
 * result in more emissions, so that could carry a performance penalty
 * @param source The source to join with
 *
 */
fun PartialPipe.joinRemote(key: (IMessage) -> String, optional: Boolean = false, source: () -> Source) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val jrt = JoinRemoteTransformer(source.invoke().toReactivePipe(), keyExtractor, false, optional)
    addTransformer(Transformer(jrt))
}

/**
 * Group a source, using the key from the lambda. The messages will be unchanged, only the key will have the supplied key pre-pended.
 */
fun PartialPipe.group(key: (IMessage) -> String) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val group = GroupTransformer(keyExtractor)
    addTransformer(Transformer(group))
}

/**
 * Use an existing source
 */
fun Pipe.source(topic: String, init: Source.() -> Unit): Source {
    val sourceElement = TopicSource(topic)
    val source = Source(sourceElement)

    source.init()
//    this.addSource(source)
    return source
}

/**
 * Creates a simple sink that will contain the result of the current transformation. Multiple sinks may be added.
 */
fun PartialPipe.sink(topic: String) {
    val sink = SinkTransformer(topic, Optional.empty())
    addTransformer(Transformer(sink))
}

/**
 * Joins with another source using the same key as this source.
 * @param optional: If set to true, it will also emit a value if there is no counterpart (yet) in the supplied source.
 * Note that setting this to true can cause a performance penalty, as more items could be emitted.
 */
fun PartialPipe.join(optional: Boolean = false,debug: Boolean = false, source: () -> Source) {
    val jrt = JoinWithTransformer(optional, false, source.invoke().toReactivePipe(),debug)
    addTransformer(Transformer(jrt))
}

/**
 * Joins with another source that has been grouped with the key of the current source.
 * This generally means that the inner source should end with a 'group' transformation
 * @param optional: If set to true, it will also emit a value if there is no counterpart (yet) in the supplied source.
 * Note that setting this to true can cause a performance penalty, as more items could be emitted.
 */
fun PartialPipe.joinGrouped(optional: Boolean = false, debug: Boolean = false, source: () -> Source) {
    val jrt = JoinWithTransformer(optional, true, source.invoke().toReactivePipe(),debug)
    addTransformer(Transformer(jrt))
}

/**
 * Scan is effectively a 'reduce' operator (The 'scan' name is used in Rx). A real reduce makes no sense in infinite streams (as it would emit
 * a single item when the stream completes, which never happens).
 * It is a little bit more complicated than a regular reduce operator, as it also allows deleting items
 *
 */
fun PartialPipe.scan(key: (IMessage) -> String, initial: (IMessage) -> IMessage, onAdd: Block.() -> Transformer, onRemove: Block.() -> Transformer) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val initialConstructor: (ImmutableMessage) -> ImmutableMessage = { msg -> initial.invoke(fromImmutable(msg)).toImmutable() }
    val onAddBlock = Block()
    onAdd.invoke(onAddBlock)
    val onRemoveBlock = Block()
    onRemove.invoke(onRemoveBlock)
    addTransformer(Transformer(ScanTransformer(keyExtractor, initialConstructor, onAddBlock.transformers.map { e -> e.component }, onRemoveBlock.transformers.map { e -> e.component })))
}

/**
 * Create a new top level stream instance
 * @param generation This is a string that indicates this current run. If you stop this run and restart it, it will continue where it left off.
 * This might take some getting used to. Usually, when you are developing, you change your code, and restart.
 * In the case of a data transformation like floodplain, it might mean that nothing will happen: All data has already been transformed.
 * To make sure all data gets reprocessed, change the generation. It is a string, with no further meaning within the framework, you can choose what
 * meaning you want to attach. You can increment a number, use a sort of time stamp, or even a git commit.
 */
fun pipe(generation: String, init: Pipe.() -> Source): Pipe {
    val tenant = "tenant"
    val deployment = "deployment"
    // this is basically the name of the pipe instance, to make sure no names clash
    val instance = "instance"
    var topologyConstructor = TopologyConstructor() // TopologyConstructor(Optional.of(AdminClient.create(mapOf("bootstrap.servers" to kafkaBrokers, "client.id" to clientId))))
    var topologyContext = TopologyContext(Optional.ofNullable(tenant), deployment, instance, generation)
    val pipe = Pipe(topologyContext, topologyConstructor)

    return pipe.addSource(pipe.init())
}

fun pipes(generation: String, init: Pipe.() -> List<Source>): Pipe {
    val tenant = "tenant"
    val deployment = "deployment"
    // this is basically the name of the pipe instance, to make sure no names clash
    val instance = "instance"
    var topologyConstructor = TopologyConstructor() // TopologyConstructor(Optional.of(AdminClient.create(mapOf("bootstrap.servers" to kafkaBrokers, "client.id" to clientId))))
    var topologyContext = TopologyContext(Optional.ofNullable(tenant), deployment, instance, generation)
    val pipe = Pipe(topologyContext, topologyConstructor)
    val sources = pipe.init()
    sources.forEach {
        e->pipe.addSource(e)
    }
    return pipe
}

/**
 * Sources wrap a TopologyPipeComponent
 */
open class Source(val component: TopologyPipeComponent) : PartialPipe() {
    fun toReactivePipe(): ReactivePipe {
        return ReactivePipe(component, transformers.map { e -> e.component })
    }
}


/**
 * Common superclass for sources or 'pipe segments', either a source, transformer or block
 */
abstract class PartialPipe {
    val transformers: MutableList<Transformer> = mutableListOf()

    fun addTransformer(transformer: Transformer): Transformer {
        transformers.add(transformer)
        return transformer
    }
}

/**
 * Concrete version of a partial pipe
 */
class Block() : PartialPipe() {
}
