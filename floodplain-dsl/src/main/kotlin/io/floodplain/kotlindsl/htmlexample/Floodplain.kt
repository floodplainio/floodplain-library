package io.floodplain.kotlindsl.htmlexample

class Floodplain {
}

abstract class Transformer {

}

class Filter(filter: (IMessage, IMessage) -> Boolean) : Transformer() {
}

class Transform(transformer: (IMessage, IMessage) -> IMessage) : Transformer() {
}

class JoinRemote(key: (IMessage,IMessage)->String, source: ()->Source) : Transformer() {
}



class Pipe() {

    private val sources: MutableList<Source> = ArrayList()

    fun addSource(source: Source) {
        sources.add(source)
    }
//    fun source(init: Source.() -> Unit): Source {
//        val source = Source()
//        source.init()
//        return source
//    }

//    fun transformer(init: Transformer.() -> Unit): Transformer {
//        val transformer = Transformer()
//        transformer.init()
//        return transformer
//    }
}

fun Source.filter(a: (IMessage,IMessage)->Boolean) {
    addTransformer(Filter(a))
}

fun Source.set(a: (IMessage,IMessage)->IMessage) {
    addTransformer(Transform(a))
}

fun Source.joinRemote(key: (IMessage,IMessage)->String, source: () -> Source) {
    addTransformer(JoinRemote(key, source))
}

fun pipe(init: Pipe.() -> Unit): Pipe {
    val pipe = Pipe()
    pipe.init()
    return pipe
}

abstract class Source {
    private val transformers: MutableList<Transformer> = mutableListOf()
//    fun transformer(init: Transformer.() -> Unit) {
//        val transformer = Transformer()
//        transformer.init()
//        transformers.add(transformer)
//    }
    fun addTransformer(transformer: Transformer) {
        transformers.add(transformer)
    }
}

class DatabaseSource(resourceName: String,schema: String, table: String): Source() {
}


fun Pipe.databaseSource(resourceName: String,schema: String, table: String,init: DatabaseSource.()->Unit):Source {
    val databaseSource = DatabaseSource(resourceName,schema,table)
    databaseSource.init()
    this.addSource(databaseSource)
    return databaseSource
}

fun main() {
    pipe {

        databaseSource("dvd","public","staff") {
            filter { msg,_ ->
                "aaa" == msg.optionalString("Blib/Blab")
            }
//            map { msg,state->
//                msg
//
//            }
            joinRemote( {msg,_->msg.integer("address_id").toString()}) {
                databaseSource("dvd","public","address") {
                    set { msg,_->
                        msg.clear("last_update")
                        msg.set("bla",3)
                        msg
                    }
                }
            }
        }
    }
}