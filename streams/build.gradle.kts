import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":streams-api"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    implementation(project(":kafka-converter"))
    implementation(FloodplainDeps.jacksonDatabind310)
    compile(FloodplainDeps.kafkaClient)
    compile(FloodplainDeps.kafkaStreams)
    compile(FloodplainDeps.kafkaConnectApi)
    // compile(FloodplainDeps.kafkaConnectFile)
    compile(FloodplainDeps.kafkaConnectRuntime)
    compile(FloodplainDeps.cdiApi)
    compile(FloodplainDeps.microProfileConfig)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testRuntimeOnly(FloodplainDeps.junitJupiterVintage)

}
