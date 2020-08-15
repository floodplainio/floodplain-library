import io.floodplain.build.FloodplainDeps
dependencies {
    implementation(FloodplainDeps.debeziumEmbedded)
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    compile(FloodplainDeps.kotlinLogging)
    testCompile(FloodplainDeps.jUnit)
    implementation(project(":floodplain-api"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation(project(":floodplain-dsl"))
    testImplementation(project(":floodplain-test"))
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(FloodplainDeps.testContainer)
}
