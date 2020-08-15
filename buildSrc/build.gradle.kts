import org.gradle.kotlin.dsl.`kotlin-dsl`
// import nl.javadude.gradle.plugins.license.DownloadLicensesExtension
// import nl.javadude.gradle.plugins.license.LicenseExtension
//import nl.javadude.gradle.plugins.license.*

plugins {
    `kotlin-dsl` apply false
    id("org.jetbrains.dokka").version("0.10.1")
    id("com.github.hierynomus.license").version("0.15.0")
}

repositories {
    mavenLocal()
    mavenCentral()
    gradlePluginPortal()
}

// configure<LicenseExtension> {
//     header = file("$rootDir/gradle/LICENSE_HEADER.txt")
//     skipExistingHeaders = false
//
//     mapping(
//         kotlin.collections.mapOf(
//             "java" to "SLASHSTAR_STYLE"
//         )
//     )
//
//     excludes(
//         kotlin.collections.listOf(
//             "**/*.json",
//
//             // Ignore copy-pasted library .js and .css's
//             "**/tempusdominus-bootstrap*",
//             "**/chosen.*",
//             "**/bootstrap-multiselect.*"
//         )
//     )
// }

tasks.dokka {
    outputFormat = "gfm"
    outputDirectory = "$buildDir/dokka"

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
