import org.gradle.kotlin.dsl.`kotlin-dsl`

buildscript {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
    }
    dependencies {
        classpath("gradle.plugin.com.hierynomus.gradle.plugins:license-gradle-plugin:0.15.0")
    }
}
plugins {
    `kotlin-dsl` apply false
    id("org.jetbrains.kotlin.jvm") version "1.3.72"
    id("org.jetbrains.dokka").version("0.10.1")

}


repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
}



// tasks {
//     val sourcesJar by creating(Jar::class) {
//         archiveClassifier.set("sources")
//         from(sourceSets.main.get().allSource)
//     }
//
//     val javadocJar by creating(Jar::class) {
//         dependsOn.add(javadoc)
//         archiveClassifier.set("javadoc")
//         from(javadoc)
//     }
//
//     artifacts {
//         archives(sourcesJar)
//         archives(javadocJar)
//         archives(jar)
//     }
// }
