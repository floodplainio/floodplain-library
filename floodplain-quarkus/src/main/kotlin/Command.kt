package io.floodplain.command
import io.floodplain.kotlindsl.postgresSourceConfig
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain

private val logger = mu.KotlinLogging.logger {}

class Test {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val cmd = Command()
            cmd.run(*args)
        }
    }
}

@QuarkusMain
class Command : QuarkusApplication {

    override fun run(vararg args: String?): Int {
        // Runtime.getRuntime().addShutdownHook(object : Thread() {
        //     override fun run() {
        //     }
        // })
        println("do something")
        stream {
            val postgresConfig = postgresSourceConfig(
                "mypostgres",
                "localhost",
                5432,
                "postgres",
                "mysecretpassword",
                "dvdrental",
                "public"
            )
            val mongoConfig = mongoConfig(
                "mongosink",
                "mongodb://localhost:27017",
                "@mongodump"
            )
            postgresConfig.sourceSimple("film") {
                mongoSink("blini", "@film", mongoConfig)
            }
        }.renderAndTest {
            logger.info("Outputs: ${outputs()}")
            Thread.sleep(20000)
        }
        println("done!")
        return 0
    }
}
