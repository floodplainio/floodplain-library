package io.floodplain.kotlindsl

import io.floodplain.kotlindsl.message.empty
import org.junit.jupiter.api.Test

private val logger = mu.KotlinLogging.logger {}

class TestMultipartition {

    @Test
    fun simpleTransformation() {
        stream {
            from("mysource") {
                transform {
                    it.set("name", "Frank")
                }
                toTopic("people")
            }
        }.renderAndExecute {
            input("mysource", "1", empty().set("species", "human"))
            val (_, value) = output("people")
            logger.info("Person found: $value")
        }
    }
}
