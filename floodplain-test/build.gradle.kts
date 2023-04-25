import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.20"
}

dependencies {
    implementation(FloodplainDeps.testContainer)
    implementation(FloodplainDeps.testContainerKafka)
    implementation(FloodplainDeps.kotlinLogging)
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