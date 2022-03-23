import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    implementation(project(":streams"))
    implementation(project(":streams-api"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-debezium"))
    implementation(project(":floodplain-direct"))
    implementation(project(":floodplain-api"))
    testImplementation(project(":floodplain-test"))
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    implementation(FloodplainDeps.debeziumEmbedded) {
        exclude(group = "log4j", module = "log4j" )
    }

    implementation(FloodplainDeps.argParser)
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        exclude(group = "log4j", module = "log4j" )
    }
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(FloodplainDeps.log4j2)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
