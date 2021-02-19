package io.floodplain.kotlindsl

import org.junit.Assert
import org.junit.Test

class TestTopicContext {

    @Test
    fun testQualifiedTopic() {
        stream(null, "mydeployment", "mygeneration") {
            val qualified = topic("sometopic")
            Assert.assertEquals("mydeployment-sometopic", qualified)
            val generational = generationalTopic("sometopic")
            Assert.assertEquals("mydeployment-mygeneration-sometopic", generational)
        }
    }
}
