package io.floodplain.integration

import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout

private val logger = mu.KotlinLogging.logger {}

suspend fun waitForMongoDbCondition(connectionString: String, database: String, check: (MongoDatabase) -> Any?): Any? {
    var returnValue: Any? = null
    MongoClients.create(connectionString)
        .use({ client ->
            val db = client.getDatabase(database)
            withTimeout(200000) {
                repeat(1000) {
                    val value = check(db)
                    if (value != null) {
                        returnValue = value
                        return@withTimeout
                    }
                    delay(100)
                }
            }
        })
        return returnValue
    }
