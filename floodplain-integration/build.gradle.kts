import io.floodplain.build.FloodplainDeps

dependencies {
    api(FloodplainDeps.slf4j)
    compile(FloodplainDeps.kotlinLogging)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":immutable-api"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-googlesheets"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.jacksonDatabind310)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(project(":floodplain-test"))
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testImplementation(FloodplainDeps.testContainer)
    testImplementation(FloodplainDeps.testContainerKafka)
    testImplementation(FloodplainDeps.debeziumTestContainers)
    testImplementation(FloodplainDeps.mongoClientSync)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testRuntimeOnly(FloodplainDeps.junitJupiterVintage)
}
