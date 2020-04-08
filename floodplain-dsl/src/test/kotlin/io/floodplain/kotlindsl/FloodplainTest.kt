package io.floodplain.kotlindsl

import com.dexels.immutable.factory.ImmutableFactory
import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.kafka.streams.remotejoin.TopologyConstructor
import io.floodplain.kotlindsl.message.empty
import java.util.*
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