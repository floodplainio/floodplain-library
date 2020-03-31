@file:Suppress("ImplicitThis")

package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty

abstract class Transformer : PartialPipe() {
//    abstract fun render()
}

class Transform(transformer: (IMessage, IMessage) -> IMessage) : Transformer() {
}

class JoinRemote(key: (IMessage) -> String, source: () -> Source) : Transformer() {
}

class JoinWith(source: () -> Source) : Transformer() {
}

class Scan(key: (IMessage) -> String, initial: () -> IMessage) : Transformer() {
    val onAdd = Block()
    val onRemove = Block()

}


class Filter(filter: (IMessage, IMessage) -> Boolean) : Transformer() {
}

//class Filter2(filter: (IMessage, IMessage) -> Boolean) : Transformer() {
//}

fun PartialPipe.filter(a: (IMessage, IMessage) -> Boolean) {
    addTransformer(Filter(a))
}

//fun PartialPipe.filter2(filter: (IMessage, IMessage) -> Boolean, init: Filter2.() -> Unit): Transformer {
//    val filter2 = Filter2(filter)
//    filter2.init()
//    this.addTransformer(filter2)
//    return filter2
//}

fun PartialPipe.set(a: (IMessage, IMessage) -> IMessage): Transformer {
    return addTransformer(Transform(a))
}

//fun PartialPipe.copy()
fun PartialPipe.joinRemote(key: (IMessage) -> String, source: () -> Source) {
    addTransformer(JoinRemote(key, source))
}

fun PartialPipe.joinWith(source: () -> Source) {
    addTransformer(JoinWith(source))
}

fun PartialPipe.scan(key: (IMessage) -> String, initial: () -> IMessage, onAdd: Block.() -> Unit, onRemove: Block.() -> Unit) {
    val scan = Scan(key, initial)
    scan.onAdd.onAdd()
    scan.onRemove.onRemove()
    addTransformer(scan)
}

fun pipe(init: Pipe.() -> Unit): Pipe {
    val pipe = Pipe()
    pipe.init()
    return pipe
}

abstract class Source : PartialPipe() {
}


abstract class PartialPipe {
    private val transformers: MutableList<Transformer> = mutableListOf()


    fun addTransformer(transformer: Transformer): Transformer {
        transformers.add(transformer)
        return transformer
    }
}

class Block() : PartialPipe() {

}

class DatabaseSource(resourceName: String, schema: String, table: String) : Source() {
}


//fun Pipe.block(init: Block.()->Unit):Source {
//    val block = Block()
//    block.init()
//    this.addSource()
//}

fun Pipe.databaseSource(resourceName: String, schema: String, table: String, init: DatabaseSource.() -> Unit): Source {
    val databaseSource = DatabaseSource(resourceName, schema, table)
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
                    scan({ msg -> msg.integer("customer_id").toString() }, { empty() },
                            { set { msg, state -> state.set("total", state.integer("total") + msg.integer("amount")); state } },
                            { set { msg, state -> state.set("total", state.integer("total") - msg.integer("amount")); state } }
                    )
                    set { _, state ->
                        empty()
                            .set("total", state.integer("total"))
                            .set("customer_id",state.integer("customer_id"))
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
    println("sources: $src")
}