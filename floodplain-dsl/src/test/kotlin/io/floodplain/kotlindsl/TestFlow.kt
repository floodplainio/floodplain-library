package io.floodplain.kotlindsl

import kotlinx.coroutines.flow.broadcastIn
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import org.junit.Test

class TestFlow {

    @Test
    fun testFlow() {
        runBlocking {
            val original = flowOf("aap", "noot", "mies", "wim", "zus", "jet", "weide", "does")
            val broadcast = original.broadcastIn(this)
            val flow1 = broadcast.openSubscription().receiveAsFlow().filter { it.length == 4 }
            val flow2 = broadcast.openSubscription().receiveAsFlow().filter { it.length == 3 }
            flow1.collect { it -> println("Four letter: $it") }
            flow2.collect { it -> println("Three letter: $it") }
        }
    }
}
