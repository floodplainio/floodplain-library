import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
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

    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)

    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testCompile(FloodplainDeps.testContainer)
}
