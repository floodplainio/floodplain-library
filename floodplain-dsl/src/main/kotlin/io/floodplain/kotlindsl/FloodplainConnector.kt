package io.floodplain.kotlindsl

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import io.floodplain.streams.api.CoreOperators
import io.floodplain.streams.api.TopologyContext
import io.floodplain.streams.remotejoin.TopologyDefinitionException
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.ProtocolException
import java.net.URL
import java.util.*
import java.util.function.Consumer

private val logger = mu.KotlinLogging.logger {}
private val objectMapper = ObjectMapper()

fun constructConnectorJson(topologyContext: TopologyContext, connectorName: String, parameters: Map<String, Any>): String {
    val generatedName = CoreOperators.topicName(connectorName, topologyContext)
    val connector = parameters["connector.class"] as String?
            ?: throw TopologyDefinitionException("Error creating connector message for connector: $connectorName it has no connector.class setting")
    val node = objectMapper.createObjectNode()
    node.put("name", generatedName)
    val configNode = objectMapper.createObjectNode()
    node.set<JsonNode>("config", configNode)
    parameters.forEach { (k: String, v: Any) ->
        if (v is String) {
            configNode.put(k, v as String?)
        } else if (v is Int) {
            configNode.put(k, v as Int?)
        } else if (v is Long) {
            configNode.put(k, v as Long?)
        } else if (v is Float) {
            configNode.put(k, v as Float?)
        } else if (v is Double) {
            configNode.put(k, v as Double?)
        } else if (v is Boolean) {
            configNode.put(k, v as Boolean?)
        }
    }
    // override name to match general name
    configNode.put("name", generatedName)
    // TODO this seems debezium specific
    configNode.put("database.server.name", generatedName)
    val jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node)
    logger.info(">> {}", jsonString)
//    postToHttp(connectURL, jsonString)
    return jsonString
}

fun startConstructor(connectorName: String, topologyContext: TopologyContext, connectURL: URL, jsonString: String, force: Boolean) {
    val generatedName = CoreOperators.topicName(connectorName, topologyContext)
    val current = existingConnectors(connectURL)
    if (current.contains(generatedName)) {
        if (force) {
            logger.warn("Force enabled, deleting old")
            deleteConnector(generatedName, connectURL)
        } else {
            logger.warn("Connector: {} already present, ignoring", generatedName)
        }
    }
    postToHttp(connectURL, jsonString)
}


private fun existingConnectors(url: URL): List<String> {
    val an = objectMapper.readTree(url.openStream()) as ArrayNode
    val result: MutableList<String> = ArrayList()
    an.forEach(Consumer { j: JsonNode -> result.add(j.asText()) })
    return Collections.unmodifiableList(result)
}


@Throws(IOException::class)
private fun deleteConnector(name: String, connectURL: URL) {
    val url = URL("$connectURL/$name")
    val con = url.openConnection() as HttpURLConnection
    con.requestMethod = "DELETE"
    val code = con.responseCode
    logger.info("Delete result: {}", code)
}

// TODO replace with Java 11 client when we can go to graal 19.3
@Throws(ProtocolException::class, IOException::class)
private fun postToHttp(url: URL, jsonString: String) {
//		URL url = new URL(this.connectURL);
    logger.info("Posting to: {}", url)
    val con = url.openConnection() as HttpURLConnection

//		-H "Accept:application/json" -H "Content-Type:application/json"
    con.requestMethod = "POST"
    con.setRequestProperty("Content-Type", "application/json")
    con.setRequestProperty("Accept", "application/json")
    con.doOutput = true
    con.outputStream.use { os ->
        val input = jsonString.toByteArray(charset("utf-8"))
        os.write(input, 0, input.size)
    }
    logger.info("Result code: {} and message: {}", con.responseCode, con.responseMessage)
    BufferedReader(
            InputStreamReader(con.inputStream, "utf-8")).use { br ->
        val response = StringBuilder()
        var responseLine: String? = null
        while (br.readLine().also { responseLine = it } != null) {
            response.append(responseLine!!.trim { it <= ' ' })
        }
        println(response.toString())
    }
}
