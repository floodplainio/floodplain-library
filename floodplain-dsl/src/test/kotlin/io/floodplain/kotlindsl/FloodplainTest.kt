package io.floodplain.kotlindsl

import io.floodplain.immutable.factory.ImmutableFactory
import io.floodplain.kotlindsl.message.empty
import kotlin.test.Test

class FloodplainTest {


    @Test
    fun parsePipe() {
        val msg = empty()
        msg["bla"] = "ble"
        msg["blieb"] = 3
        val serialized = msg.toImmutable().toFlatString(ImmutableFactory.createParser())
        println("Serialized: $serialized")
    }
}