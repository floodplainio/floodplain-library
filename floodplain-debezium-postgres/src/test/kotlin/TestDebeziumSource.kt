package io.floodplain.debezium.postgres

import java.io.File
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Test
import org.testcontainers.containers.GenericContainer

class InstantiatedContainer(image: String, port: Int, env: Map<String, String> = emptyMap()) {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    var container: KGenericContainer?
    var host: String
    var exposedPort: Int = -1
    init {
        container = KGenericContainer(image)
            .apply { withExposedPorts(port) }
            .apply { withEnv(env) }
        container?.start()
        host = container?.host ?: "localhost"
        exposedPort = container?.firstMappedPort ?: -1
    }
    fun close() {
        container?.close()
    }
}
class TestDebeziumSource {

    private val postgresContainer = InstantiatedContainer("floodplain/floodplain-postgres-demo:1.0.0", 5432)

    @Test
    fun testShortRun() {
        val offsets = File.createTempFile("temp", null).toPath()
        runBlocking {
            postgresDataSource("mypostgres", postgresContainer.host, postgresContainer.exposedPort, "dvdrental", "postgres", "mysecretpassword", offsets,
                emptyMap())
                .take(500)
                .collect { it.key
                    println("Topic: ${it.topic} key: ${it.key}")
                }
            println("completed")
        }
        offsets.toFile().delete()
    }

    @After
    fun shutdown() {
        postgresContainer.close()
    }
}
