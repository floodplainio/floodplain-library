import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    compile(FloodplainDeps.slf4j)
    compile(FloodplainDeps.jacksonCore)
    compile(FloodplainDeps.jacksonDatabind)
    compile(FloodplainDeps.cdiApi)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testRuntimeOnly(FloodplainDeps.junitJupiterVintage)
}
