import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-api"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-debezium"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testCompile(FloodplainDeps.testContainer)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
