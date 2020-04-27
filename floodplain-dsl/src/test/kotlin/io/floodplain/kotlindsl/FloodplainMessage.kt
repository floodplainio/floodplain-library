package io.floodplain.kotlindsl

import io.floodplain.immutable.factory.ImmutableFactory
import io.floodplain.kotlindsl.message.empty
import kotlin.test.Test

class FloodplainMessage {


    @Test
    fun testMessageConversion() {
        val msg = empty()
        msg["bla"] = "ble"
        msg["blieb"] = 3
        val serialized = msg.toImmutable().toFlatString(ImmutableFactory.createParser())
        println("Serialized: $serialized")
    }
}