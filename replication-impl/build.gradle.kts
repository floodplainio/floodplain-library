import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.slf4j)
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
