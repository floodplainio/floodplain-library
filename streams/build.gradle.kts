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
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaClient)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kafkaConnectApi)
    // implementation(FloodplainDeps.kafkaConnectFile)
    implementation(FloodplainDeps.kafkaConnectRuntime) {
        // exclude(group = "log4j", module = "log4j" )
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender" )
    }
    implementation(FloodplainDeps.kafkaTools) {
        // exclude(group = "log4j", module = "log4j" )
        exclude(group = "org.apache.kafka", module = "kafka-log4j-appender" )
    }
    implementation(FloodplainDeps.cdiApi)
    implementation(FloodplainDeps.microProfileConfig)

}
