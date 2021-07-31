
import io.floodplain.build.FloodplainDeps

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    compile(FloodplainDeps.kafkaStreams)
    compile(FloodplainDeps.kafkaConnectApi)
}
