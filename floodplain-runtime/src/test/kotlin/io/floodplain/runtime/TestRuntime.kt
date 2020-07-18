package io.floodplain.runtime

import com.xenomachina.argparser.ArgParser
import org.junit.Assert
import org.junit.Test

class TestRuntime {

    @Test
    fun testArgs() {
        val args = arrayOf("--kafka", "localhost:9092")
        ArgParser(args).parseInto(::LocalArgs).run {
            Assert.assertEquals("localhost:9092", kafka)
            Assert.assertNull(connect)
            Assert.assertFalse(force)
        }
        val argsWithConnect = arrayOf("--kafka", "localhost:9092", "--connect", "http://localhost:8083", "--force")
        ArgParser(argsWithConnect).parseInto(::LocalArgs).run {
            Assert.assertEquals("localhost:9092", kafka)
            Assert.assertEquals("http://localhost:8083", connect)
            Assert.assertTrue(force)
        }
    }
}
