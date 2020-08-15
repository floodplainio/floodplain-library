import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    testCompile(FloodplainDeps.jUnit)
}
