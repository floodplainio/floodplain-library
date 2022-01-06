import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.slf4j)
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":streams-api"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    implementation(project(":kafka-converter"))
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaClient)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kafkaConnectApi)
    // implementation(FloodplainDeps.kafkaConnectFile)
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(FloodplainDeps.cdiApi)
    implementation(FloodplainDeps.microProfileConfig)

}
