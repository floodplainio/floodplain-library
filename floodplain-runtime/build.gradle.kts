dependencies {
    implementation(project(":floodplain-dsl"))
//    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(FloodplainDeps.argParser)
    testImplementation(FloodplainDeps.jUnit)
}
