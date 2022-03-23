import io.floodplain.build.FloodplainDeps

dependencies {
    api(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":immutable-api"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-jdbc"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-googlesheets"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaClient)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kafkaConnectApi)
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        exclude(group = "log4j", module = "log4j" )
    }
    implementation(FloodplainDeps.commonsCompress) // update for vulnerability
    testImplementation(project(":floodplain-test"))
    testImplementation(FloodplainDeps.log4j2)
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testImplementation(FloodplainDeps.testContainer)
    testImplementation(FloodplainDeps.testContainerKafka)
    testImplementation(FloodplainDeps.debeziumTestContainers)
    testImplementation(FloodplainDeps.mongoClientSync)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
