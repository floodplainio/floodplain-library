import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.testContainer)
    implementation(FloodplainDeps.testContainerKafka)
    implementation(FloodplainDeps.kotlinLogging)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
}
