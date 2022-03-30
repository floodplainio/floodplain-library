import io.floodplain.build.FloodplainDeps
dependencies {
    implementation(FloodplainDeps.debeziumEmbedded) {
        // exclude(group = "log4j", module = "log4j" )
    }
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-api"))
    implementation(FloodplainDeps.kotlinCoroutines)

    testImplementation(FloodplainDeps.log4j2)
    // testImplementation(FloodplainDeps.log4j2OverSlf)
    testImplementation(project(":floodplain-dsl"))
    testImplementation(project(":floodplain-test"))
    testImplementation(FloodplainDeps.testContainer)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
