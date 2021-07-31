import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":floodplain-dsl"))
    implementation(FloodplainDeps.argParser)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
}
