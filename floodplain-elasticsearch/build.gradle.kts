import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-test"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    testCompile(FloodplainDeps.testContainer)
}
