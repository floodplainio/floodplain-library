import io.floodplain.build.FloodplainDeps

dependencies {
    compile(FloodplainDeps.jacksonCore)
    compile(FloodplainDeps.jacksonDatabind)
    compile(FloodplainDeps.slf4j)
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
}
