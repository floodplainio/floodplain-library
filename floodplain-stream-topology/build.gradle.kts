import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":streams"))
    implementation(project(":streams-api"))
    implementation(project(":kafka-converter"))
    implementation(FloodplainDeps.slf4j)
    testImplementation(FloodplainDeps.jUnit)
    testImplementation(FloodplainDeps.slf4jLog4j)
}
