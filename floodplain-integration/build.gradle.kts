import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
}

dependencies {
    api(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":immutable-api"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-jdbc"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-googlesheets"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.jacksonDatabind310)
    implementation(FloodplainDeps.kafkaClient)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.kafkaConnectApi)
    implementation(FloodplainDeps.kafkaConnectRuntime)
    implementation(FloodplainDeps.commonsCompress) // update for vulnerability
    testImplementation(project(":floodplain-test"))
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testImplementation(FloodplainDeps.testContainer)
    testImplementation(FloodplainDeps.testContainerKafka)
    testImplementation(FloodplainDeps.debeziumTestContainers)
    testImplementation(FloodplainDeps.mongoClientSync)
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