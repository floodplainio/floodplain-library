@file:Suppress("ImplicitThis")

package io.floodplain.kotlindsl

import com.dexels.immutable.api.ImmutableMessage
import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import com.dexels.navajo.reactive.source.topology.*
import com.dexels.navajo.reactive.source.topology.api.TopologyPipeComponent
import com.dexels.navajo.reactive.topology.ReactivePipe
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.message.fromImmutable
import org.apache.kafka.streams.Topology
import java.util.*

open class Transformer(val component: TopologyPipeComponent) : PartialPipe() {
}


//class Filter2(filter: (IMessage, IMessage) -> Boolean) : Transformer() {
//}

fun PartialPipe.filter(flt: (IMessage, IMessage) -> Boolean) {
    val transformerFilter: (ImmutableMessage, ImmutableMessage) -> Boolean = { msg: ImmutableMessage, param: ImmutableMessage -> flt.invoke(fromImmutable(msg), fromImmutable(param)) }
    val transformer = FilterTransformer(transformerFilter)
    addTransformer(Transformer(transformer))
}

//fun PartialPipe.filter2(filter: (IMessage, IMessage) -> Boolean, init: Filter2.() -> Unit): Transformer {
//    val filter2 = Filter2(filter)
//    filter2.init()
//    this.addTransformer(filter2)
//    return filter2
//}

fun PartialPipe.set(transform: (IMessage, IMessage) -> IMessage): Transformer {
    val transformer: (ImmutableMessage, ImmutableMessage) -> ImmutableMessage = { msg: ImmutableMessage, param: ImmutableMessage -> transform.invoke(fromImmutable(msg), fromImmutable(param)).toImmutable() }
    val set = SetTransformer(transformer)
    return addTransformer(Transformer(set))
}

//fun PartialPipe.copy()
fun PartialPipe.joinRemote(key: (IMessage) -> String, source: () -> Source) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }
    val jrt = JoinRemoteTransformer(source.invoke().toReactivePipe(), keyExtractor)
    addTransformer(Transformer(jrt))
}

fun PartialPipe.joinWith(source: () -> Source) {
    val jrt = JoinWithTransformer(source.invoke().toReactivePipe(), Optional.empty())
    addTransformer(Transformer(jrt))
}

fun PartialPipe.scan(key: (IMessage) -> String, initial: () -> IMessage, onAdd: Block.() -> Transformer, onRemove: Block.() -> Transformer) {
    val keyExtractor: (ImmutableMessage, ImmutableMessage) -> String = { msg, _ -> key.invoke(fromImmutable(msg)) }

//    val scan = Scan(key, initial)
    val onAddBlock = Block()
    onAdd.invoke(onAddBlock)
    val onRemoveBlock = Block()
    onRemove.invoke(onRemoveBlock)

//    val bif : BiFunction<ImmutableMessage,ImmutableMessage,Optional<String>> = BiFunction( {
//
//    })
//    public ScanTransformer(java.util.Optional<BiFunction<ImmutableMessage, ImmutableMessage, String>> keyExtractor, ImmutableMessage initial, List<TopologyPipeComponent> onAdd, List<TopologyPipeComponent> onRemove) {
    val scanTransformer = ScanTransformer(keyExtractor, initial.invoke().toImmutable(), onAddBlock.transformers.map { e -> e.component }, onRemoveBlock.transformers.map { e -> e.component })

//    scan.onAdd.onAdd()
//    scan.onRemove.onRemove()
//    addTransformer(scan)
}

fun pipe(init: Pipe.() -> Unit): Pipe {
    val pipe = Pipe()
    pipe.init()
    return pipe
}

open class Source(val component: TopologyPipeComponent) : PartialPipe() {
    fun toReactivePipe(): ReactivePipe {
        return ReactivePipe(component, transformers.map { e -> e.component })
    }
}


abstract class PartialPipe {
    val transformers: MutableList<Transformer> = mutableListOf()
//    fun setTopologyPipeComponent(tpc: TopologyPipeComponent) {
//
//    }


    fun addTransformer(transformer: Transformer): Transformer {
        transformers.add(transformer)
        return transformer
    }
}

class Block() : PartialPipe() {

}


//fun Pipe.block(init: Block.()->Unit):Source {
//    val block = Block()
//    block.init()
//    this.addSource()
//}

fun Pipe.databaseSource(resourceName: String, schema: String, table: String, init: Source.() -> Unit): Source {
    val databaseSource = Source(DebeziumTopic(table, schema, resourceName, true, true))
    databaseSource.init()
    this.addSource(databaseSource)
    return databaseSource
}

fun main() {
//    |>database(resource='dvd',schema='public',table='customer')
//    ->joinWith(|>database(resource='dvd',schema='public',table='payment')
//    ->scan(key=ToString([customer_id])
//            , Msg(total=ToDouble(0))
//            ,->set(@total=[@total]+[amount],@customer_id=[customer_id])
//            ,->set(@total=[@total]-[amount],@customer_id=[customer_id])
//    )
//    ->only(total=[@total], customer_id=[@customer_id])
//    )
//    ->set(total=[@total],customer_id=[@customer_id])
//    ->rownum()
//    ->sink(connector='@staffsheets','@CUSTOMER',connect=true,columns='customer_id,last_name,first_name,email,total')

    val myPipe = pipe {
        databaseSource("dvd", "public", "customer") {
            joinWith {
                databaseSource("dvd", "public", "payment") {
                    scan({ msg -> msg.integer("customer_id").toString() }, { empty().set("total", 0) },
                            { set { msg, state -> state.set("total", state.integer("total") + msg.integer("amount")); state } },
                            { set { msg, state -> state.set("total", state.integer("total") - msg.integer("amount")); state } }
                    )
                    set { _, state ->
                        empty()
                                .set("total", state.integer("total"))
                                .set("customer_id", state.integer("customer_id"))
                    }
                }
            }
        }
        databaseSource("dvd", "public", "staff") {
            filter { msg, _ ->
                "aaa" == msg.optionalString("Blib/Blab")
            }
            joinRemote({ msg -> msg.integer("address_id").toString() }) {
                databaseSource("dvd", "public", "address") {
                    set { msg, _ ->
                        msg.clear("last_update")
                        msg.set("bla", 3)
                        msg
                    }
                }
            }
        }
    }

    val src = myPipe.sources().size
    var topology = Topology()
    var topologyContext = TopologyContext(Optional.of("TENANT"), "test", "instance", "20200401")
    var topologyConstructor = TopologyConstructor(Optional.empty(), Optional.empty())
//    ReactivePipeParser.processPipe(topologyContext, topologyConstructor, topology, int, Stack<String>, ReactivePipe, boolean)

    myPipe.render(topology, topologyContext, topologyConstructor)

    println("sources: ${topology.describe()}")
}