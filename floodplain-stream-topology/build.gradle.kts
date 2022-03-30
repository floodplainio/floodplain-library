import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":streams"))
    implementation(project(":streams-api"))
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kafkaStreams)
    // testImplementation(FloodplainDeps.log4j2OverSlf)
    testImplementation(FloodplainDeps.log4j2)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
