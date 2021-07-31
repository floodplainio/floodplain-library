import io.floodplain.build.FloodplainDeps
dependencies {
    implementation(FloodplainDeps.debeziumEmbedded)
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    compile(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-api"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(project(":floodplain-dsl"))
    testImplementation(project(":floodplain-test"))
    testImplementation(FloodplainDeps.testContainer)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testImplementation("org.junit.platform:junit-platform-runner:1.7.2")
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
}
