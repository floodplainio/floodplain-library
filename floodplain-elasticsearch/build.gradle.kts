import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    testCompile(FloodplainDeps.jUnit)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-test"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testCompile(FloodplainDeps.testContainer)
}
