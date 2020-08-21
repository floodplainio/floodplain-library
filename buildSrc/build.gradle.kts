import org.gradle.kotlin.dsl.`kotlin-dsl`

plugins {
    `kotlin-dsl` apply false
    id("org.jetbrains.kotlin.jvm") version "1.4.0"
    id("org.jetbrains.dokka").version("0.10.1")
}

repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
}