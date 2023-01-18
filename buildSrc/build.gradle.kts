import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val buildKotlin = "1.4.32"

plugins {
    `kotlin-dsl`
    id("io.gitlab.arturbosch.detekt").version ("1.20.0")
    kotlin("jvm") version "1.7.10"

}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
    }
}

repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
    // maven("https://maven.pkg.jetbrains.space/public/p/kotlinx-html/maven")
}
dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlin:kotlin-serialization")
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "1.8"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "1.8"
}