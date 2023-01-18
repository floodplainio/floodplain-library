import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
}
dependencies {
    implementation(FloodplainDeps.debeziumEmbedded)
    implementation(FloodplainDeps.debeziumPostgres)
    implementation(FloodplainDeps.debeziumMySQL)
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-api"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(project(":floodplain-dsl"))
    testImplementation(project(":floodplain-test"))
    testImplementation(FloodplainDeps.testContainer)
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