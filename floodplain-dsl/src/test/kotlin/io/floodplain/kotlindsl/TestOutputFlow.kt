package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import io.floodplain.streams.api.Topic
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import org.junit.Test

class TestOutputFlow {

    @Test
    fun testChunkin() {
        val flow: Flow<Triple<Topic, String, IMessage?>> = flow {
            repeat(10) {
                val topic = Topic.from("TopicNr$it")
                repeat(100) { msgNr ->
                    val msg = empty().set("element", msgNr)
                    emit(Triple(topic, "key$msgNr", msg))
                }
            }
        }

        runBlocking {
            var initialCount = 0
            val count = flow.onEach { initialCount++ }
                .bufferTimeout(7, 1000)
                // .chunkFlow(10).onEach {(topic,elements)->
                // println("Topic: $topic elements: ${elements.size}")
                // }
                .map { elts -> elts.size }
                .onEach { e -> println("EE: $e") }
                .fold(0, { acc, cnt -> acc + cnt })

            println("total: $count total: $initialCount")
        }
    }
}
