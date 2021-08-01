import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    compile(FloodplainDeps.slf4j)
    compile(FloodplainDeps.jacksonCore)
    compile(FloodplainDeps.jacksonDatabind)
    compile(FloodplainDeps.cdiApi)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
}
