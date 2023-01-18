import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    // Apply the java-library plugin to add support for Java Library
    id("java")
    id("application")
    kotlin("jvm") version "1.7.10"
}

dependencies {
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-googlesheets"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-jdbc"))

    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":replication-api"))
    implementation(project(":replication-json"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    implementation(FloodplainDeps.slf4j)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(FloodplainDeps.kafkaStreams)
    implementation(FloodplainDeps.commonsCompress) // update for vulnerability
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