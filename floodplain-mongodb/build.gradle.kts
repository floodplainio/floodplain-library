import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.7.0")
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        exclude(group = "log4j", module = "log4j" )
    }
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.mongoClientSync)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.commonsCompress) // update for vulnerability
    testImplementation(FloodplainDeps.testContainer)
    testImplementation(FloodplainDeps.log4j2)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
