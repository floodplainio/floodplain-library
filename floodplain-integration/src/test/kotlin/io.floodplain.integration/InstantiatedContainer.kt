package io.floodplain.integration

import org.testcontainers.containers.GenericContainer

class InstantiatedContainer(private val image: String, port: Int, env: Map<String, String> = emptyMap()) {

    class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)
    lateinit var container: KGenericContainer
    var host: String
    var exposedPort: Int = -1
    init {
        container = KGenericContainer(image)
            .apply { withExposedPorts(port) }
            .apply { withEnv(env) }
        container.start()
        host = container.host
        exposedPort = container.firstMappedPort
    }
    fun close() {
        container?.close()
    }
}
