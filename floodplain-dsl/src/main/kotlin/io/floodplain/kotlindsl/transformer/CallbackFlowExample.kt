package io.floodplain.kotlindsl.transformer

import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking

class Callback(val callback: (event: String)->Unit) {
    fun run() {
        for (i in 0..10) {
            callback.invoke("event$i")
            println("called: event$i")
        }
        println("done")
    }
}

fun createFlow(): Flow<String> {
    return callbackFlow {
        Callback {
            println("befor sendblocking")
            sendBlocking(it)
            println("after sendblocking")
        }.run()
        // Our sacrificial thread
//        Thread { callback.run()}.start()
//         callback.run()
        println("called start")
        awaitClose {
            // make sure the thread stops somehow
        }
    }
}

fun main() {
    runBlocking {
        val flow = createFlow();
        println("flow created")
        delay(10000)
        flow.collect({e->println(e)})
    }
}

fun main3() {
    val api = Callback{e->println("result>$e")}
    api.run()
}