import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.6.1")
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.mongoClientSync)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(FloodplainDeps.testContainer)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
