import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
}

dependencies {
    implementation(project(":immutable-api"))
    implementation(project(":immutable-impl"))
    implementation(project(":replication-api"))
    implementation(project(":replication-impl"))
    implementation(project(":replication-json"))
    implementation(project(":replication-protobuf"))
    implementation(project(":streams"))
    implementation(project(":streams-api"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-debezium"))
    implementation(project(":floodplain-direct"))
    implementation(project(":floodplain-api"))
    testImplementation(project(":floodplain-test"))
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    implementation(FloodplainDeps.debeziumEmbedded)
    implementation(FloodplainDeps.argParser)
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    implementation(kotlin("stdlib-jdk8"))
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}