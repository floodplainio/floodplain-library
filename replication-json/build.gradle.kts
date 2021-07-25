import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project("::streams-api"))
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testRuntimeOnly(FloodplainDeps.junitJupiterVintage)
}
