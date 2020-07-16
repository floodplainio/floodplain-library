package io.floodplain.debezium.postgres

import io.debezium.engine.ChangeEvent
import io.debezium.engine.DebeziumEngine
import io.debezium.engine.format.Json
import io.floodplain.ChangeRecord
import java.nio.file.Path
import java.util.Properties
import java.util.UUID
import kotlin.system.measureTimeMillis
import org.junit.Test

private val logger = mu.KotlinLogging.logger {}

class TestMySQL {
    @Test
    fun testMySql() {
        val offsetFilePath = createOffsetFilePath()
        logger.info("Creating offset files at: $offsetFilePath")
        val props = Properties()
        props.setProperty("name", "engine")
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector")
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        props.setProperty("offset.storage.file.filename", offsetFilePath.toString())
        props.setProperty("offset.flush.interval.ms", "1000")
        props.setProperty("database.hostname", "localhost")
        props.setProperty("database.port", "3306")
        props.setProperty("database.server.name", "instance-mysqlsource")
        props.setProperty("database.dbname", "my-wpdb")
        props.setProperty("database.user", "root")
        props.setProperty("database.password", "mysecretpassword")
        props.setProperty("database.server.id", "${System.currentTimeMillis()}")

        props.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory")
        props.setProperty("database.history.file.filename", "currenthistory")
        val engine = DebeziumEngine.create(Json::class.java)
            .using(props)
            .notifying { record: ChangeEvent<String, String> ->
                    val perf = measureTimeMillis {
                        send(
                            ChangeRecord(
                                record.destination(),
                                record.key(),
                                record.value()?.toByteArray()
                            )
                        )
                    }
                    if (perf > 1000) {
                        logger.debug("Send ran for: $perf")
                    }
            }
            .build()
            engine.run()
    }

    private fun send(changeRecord: ChangeRecord) {
        TODO("Not yet implemented")
    }

    private fun createOffsetFilePath(offsetId: String? = null): Path {
        val tempFile = createTempFile(offsetId ?: UUID.randomUUID().toString().substring(0, 7))
        if (offsetId == null) {
            tempFile.deleteOnExit()
        }
        return tempFile.toPath()
    }
}
