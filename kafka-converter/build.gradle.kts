
import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(FloodplainDeps.jacksonCore)
    implementation(FloodplainDeps.jacksonDatabind)
    implementation(FloodplainDeps.slf4j)
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kafkaConnectApi)
}
