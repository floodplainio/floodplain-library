
val buildKotlin = "1.4.32"

plugins {
    `kotlin-dsl`
    id("io.gitlab.arturbosch.detekt").version ("1.14.1")

}

repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
    // maven("https://maven.pkg.jetbrains.space/public/p/kotlinx-html/maven")
}
