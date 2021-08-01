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
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
