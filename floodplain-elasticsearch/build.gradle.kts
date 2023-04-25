import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.20"
}

dependencies {
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-test"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.kafkaConnectApi)
    testImplementation(FloodplainDeps.jacksonCore)
    testImplementation(FloodplainDeps.jacksonDatabind)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    testImplementation(FloodplainDeps.testContainer)
    implementation(kotlin("stdlib-jdk8"))
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "17"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "17"
}