package io.floodplain.build

object FloodplainPlugins {
    const val kotlin = "1.8.20"
}

object Libs {
    const val kotlin = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${FloodplainPlugins.kotlin}"
}

fun isReleaseVersion(): Boolean {
    return !FloodplainDeps.floodplain_version.endsWith("SNAPSHOT")
}

object FloodplainDeps {
    const val kotlin = FloodplainPlugins.kotlin
    const val floodplain_version = "1.13.1-SNAPSHOT"
    const val jackson_version = "2.15.0"
    const val kafka_version = "3.3.1!!"
    const val slf4j_version = "1.7.36"
    const val mysql_version = "8.0.32"
    const val mongodb_version = "4.9.1"
    const val debezium_version = "2.1.1.Final"
    const val testContainer_version = "1.18.0"
    const val kotlin_coroutines_version = "1.6.4"
    const val junit_5_version     = "5.9.2"
    const val debeziumPostgres    = "io.debezium:debezium-connector-postgres:$debezium_version"
    const val debeziumMySQL       = "io.debezium:debezium-connector-mysql:$debezium_version"
    const val debeziumEmbedded    = "io.debezium:debezium-embedded:$debezium_version"
    const val cdiApi              = "jakarta.enterprise:jakarta.enterprise.cdi-api:2.0.2"
    const val microProfileConfig  = "org.eclipse.microprofile.config:microprofile-config-api:1.3"
    const val mongoClientSync     = "org.mongodb:mongodb-driver-sync:$mongodb_version"
    const val slf4j               = "org.slf4j:slf4j-api:$slf4j_version"
    const val slf4jLog4j          = "org.slf4j:slf4j-log4j12:$slf4j_version"
    const val protobuf            = "com.google.protobuf:protobuf-java:3.22.2"
    const val kotlinLogging       = "io.github.microutils:kotlin-logging:3.0.5"
    const val commonsCompress     = "org.apache.commons:commons-compress:1.23.0"
    const val jacksonCore         = "com.fasterxml.jackson.core:jackson-core:$jackson_version"
    const val jacksonDatabind     = "com.fasterxml.jackson.core:jackson-databind:$jackson_version"
    const val jacksonDatabind310  = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jackson_version"
    const val kafkaClient         = "org.apache.kafka:kafka-clients:$kafka_version"
    const val kafkaConnectApi     = "org.apache.kafka:connect-api:$kafka_version"
    const val kafkaConnectFile    = "org.apache.kafka:connect-file:$kafka_version"
    const val kafkaConnectRuntime = "org.apache.kafka:connect-runtime:$kafka_version"
    const val kafkaStreams        = "org.apache.kafka:kafka-streams:$kafka_version"
    const val testContainer       = "org.testcontainers:testcontainers:$testContainer_version"
    const val testContainerKafka  = "org.testcontainers:kafka:$testContainer_version"
    const val kotlinCoroutines    = "org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlin_coroutines_version"
}
