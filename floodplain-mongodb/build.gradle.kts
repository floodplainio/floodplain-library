import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    implementation("org.mongodb.kafka:mongo-kafka-connect:1.6.0")
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.mongoClientSync)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testCompile(FloodplainDeps.testContainer)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
