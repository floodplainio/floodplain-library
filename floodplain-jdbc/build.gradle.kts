import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        exclude(group = "log4j", module = "log4j" )
    }
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-api"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-debezium"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(FloodplainDeps.testContainer)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
