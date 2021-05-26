import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    testCompile(FloodplainDeps.jUnit)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.5.0")
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.mongoClientSync)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testCompile(FloodplainDeps.testContainer)
}
