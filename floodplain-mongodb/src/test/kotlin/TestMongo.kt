import com.mongodb.client.MongoClients
import io.floodplain.kotlindsl.each
import io.floodplain.kotlindsl.filter
import io.floodplain.kotlindsl.message.empty
import io.floodplain.kotlindsl.source
import io.floodplain.kotlindsl.stream
import io.floodplain.mongodb.mongoConfig
import io.floodplain.mongodb.mongoSink
import java.util.Date
import kotlin.test.assertEquals
import kotlinx.coroutines.delay
import org.bson.Document
import org.junit.Test
import org.testcontainers.containers.GenericContainer

private val logger = mu.KotlinLogging.logger {}

class TestMongo {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var address: String? = "localhost"
    var port: Int? = 0

    var container: GenericContainer<*>? = null

    init {

        // var envVar = System.getenv("EMBED_CONTAINER")
            container = KGenericContainer("mongo:latest")
                .apply { withExposedPorts(27017) }
            container?.start()
            address = container?.getHost()
            port = container?.getFirstMappedPort()
    }

    @Test
    fun testSink() {
        stream {
            val config = mongoConfig("mongoClient", "mongodb://$address:$port", "mongo-connect-test")

            source("sometopic") {
                each { _, msg, _ -> logger.info("message: $msg") }
                filter { _, msg -> msg.long("counter") % 2 == 0L }
                mongoSink("test-collection", "myindex", config)
            }
        }.renderAndTest {
            // delay(5000)
            MongoClients.create("mongodb://$address:$port").use { client ->
                val collection = client.getDatabase("mongo-connect-test")
                    .getCollection("test-collection")
                collection.deleteMany(Document())

                repeat(100) {
                    val trip = empty().set("body", "I am a fluffy rabbit number $it and I have fluffy feet")
                        .set("time", Date().time)
                        .set("counter", it.toLong())
                    input("sometopic", "somekey_$it", trip)
                }
                val elements = outputSize("myindex")
                logger.info("Elements: $elements")
                flushSinks()
                delay(500)
                assertEquals(50L, elements)

                var doccount = collection
                    .countDocuments()
                logger.info("Count of Documents: $doccount")
                assertEquals(50L, doccount)
            }
        }
    }
}
