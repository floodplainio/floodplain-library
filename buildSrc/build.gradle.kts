import org.gradle.kotlin.dsl.`kotlin-dsl`

plugins {
    `kotlin-dsl`
    id("org.jetbrains.kotlin.jvm").version("1.4.20")
    id("org.jetbrains.dokka").version("1.4.10.2")
    id("io.gitlab.arturbosch.detekt").version ("1.14.1")

}

repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
}