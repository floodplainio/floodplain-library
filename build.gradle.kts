import io.floodplain.build.FloodplainDeps
import nl.javadude.gradle.plugins.license.*
plugins {
    id("eclipse")
    id("org.jetbrains.kotlin.jvm") version "1.3.72"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.3.72"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("org.jlleitschuh.gradle.ktlint") version "9.2.1"
    id("com.palantir.graal") version "0.7.0-5-g838c2ab"
    id("org.jetbrains.dokka") version "0.10.1"
    id("com.github.hierynomus.license").version("0.15.0")
    id("com.github.hierynomus.license-report").version("0.15.0")
    `maven-publish`
    `java-library`
}

dependencies {
    implementation(io.floodplain.build.Libs.kotlin)
}

// apply(from="gradle/dependencies.gradle")
// apply(from="buildSrc/build.gradle.kts")

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
        google()
        maven {
            url = uri("http://packages.confluent.io/maven")
        }
    }

    // license {
    //     header = rootProject.file("gradle/LICENSE_HEADER.txt")
    //     mapping("java", "SLASHSTAR_STYLE")
    //     mapping("kt", "SLASHSTAR_STYLE")
    //     // ext.year = "2020"
    //     // ext.name = "Frank Lyaruu"
    //     exclude("**/*.json")
    // }
}

// tasks.compileKotlin {
//     kotlinOptions.jvmTarget = "11"
//     kotlinOptions.javaParameters = true
//     kotlinOptions {
//         freeCompilerArgs = listOfNotNull(
//             "-Xjsr305=strict",
//             "-Xjvm-default=enable",
//             "-progressive",
//             "-Xopt-in=kotlin.RequiresOptIn",
//             "-Xopt-in=kotlin.ExperimentalUnsignedTypes",
//             "-Xopt-in=kotlin.ExperimentalStdlibApi",
//             "-Xopt-in=kotlinx.coroutines.FlowPreview",
//             "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
//             "-Xopt-in=kotlin.time.ExperimentalTime")
//     }
// }
// tasks.compileTestKotlin {
//     kotlinOptions.jvmTarget = "11"
//     kotlinOptions.javaParameters = true
//     kotlinOptions {
//         freeCompilerArgs = listOfNotNull(
//             "-Xjsr305=strict",
//             "-Xjvm-default=enable",
//             "-progressive",
//             "-Xopt-in=kotlin.RequiresOptIn",
//             "-Xopt-in=kotlin.ExperimentalUnsignedTypes",
//             "-Xopt-in=kotlin.ExperimentalStdlibApi",
//             "-Xopt-in=kotlinx.coroutines.FlowPreview",
//             "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
//             "-Xopt-in=kotlin.time.ExperimentalTime")
//     }
// }
// BOM publication doesn't work well yet.
// publishing {
//
//     repositories {
//         maven {
//             name = "Snapshots"
//             url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
//
//             credentials {
//                 username = project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")
//                 password = project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")
//             }
//         }
//         maven {
//             name = "Staging"
//             url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
//
//             credentials {
//                 username = project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")
//                 password = project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")
//             }
//         }
//     }
//     publications {
//         "$project.name"(MavenPublication) {
//             // customizePom(pom)
//             groupId = "io.floodplain"
//             artifactId = project.name
//             version = rootProject.ext.floodplain_version
//             from(components.java)
//             artifact sourceJar
//                 artifact (javadocJar)
//         }
//     }
// }
// publishing {
//     rep
//         publications {
//         create<MavenPublication>("maven") {
//             from(components["java"])
//             // artifact(sourcesJar)
//             artifact(dokkaJar)
//             // pom {
//                 //     customizePom(pom)
//                 //     groupId = "io.floodplain"
//                 //     artifactId = "library"
//                 //     version = rootProject.ext["floodplain_version"] as String
//                 //     pom.withXml {
//                 //         asNode().children().last() + {
//                 //             resolveStrategy = Closure.DELEGATE_FIRST
//                 //             dependencyManagement {
//                 //                 dependencies {
//                 //                     project.configurations.forEach { conf ->
//                 //                         conf.dependencies.forEach { dep ->
//                 //                             dependency {
//                 //                                 groupId("${dep.group}")
//                 //                                 artifactId("${dep.name}")
//                 //                                 version("${dep.version}")
//                 //                             }
//                 //                         }
//                 //                     }
//                 //                 }
//                 //             }
//                 //         }
//                 //     }
//             // }
//         }
//     }
// }

subprojects {
//    systemProperty "release", findProperty("release")
    version = FloodplainDeps.floodplain_version
//     version = FloodplainDeps.floodplain_version
    apply(plugin = "java")
    apply(plugin = "eclipse")
    apply(plugin = "maven-publish")
    apply(plugin = "distribution")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "com.github.hierynomus.license-base")

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
        kotlinOptions.jvmTarget = "11"
        kotlinOptions.javaParameters = true
        kotlinOptions {
            freeCompilerArgs = listOfNotNull(
                "-Xjsr305=strict",
                "-Xjvm-default=enable",
                "-progressive",
                "-Xopt-in=kotlin.RequiresOptIn",
                "-Xopt-in=kotlin.ExperimentalUnsignedTypes",
                "-Xopt-in=kotlin.ExperimentalStdlibApi",
                "-Xopt-in=kotlinx.coroutines.FlowPreview",
                "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
                "-Xopt-in=kotlin.time.ExperimentalTime")
        }
    }
    configure<LicenseExtension> {
        header = file("$rootDir/gradle/LICENSE_HEADER.txt")
        skipExistingHeaders = false
        val settings = HashMap<String, String>()
        val exclude = ArrayList<String>()
        exclude.add("**/*.json")
        settings.put("java", "SLASHSTAR_STYLE")
        mapping(
            settings
        )
        excludes(
            exclude
        )
    }

    apply(plugin = "signing")
    group = "io.floodplain"
    // project.publishing.publications.withType(MavenPublication::class.java).forEach { publication ->
    //     with(publication.pom) {
    //     }
    // }
    // if (name == "replication-protobuf") {
    // apply(plugin="com.google.protobuf")
    // }

    // project.publishing.publications.withType(MavenPublication::class.java).forEach { publication ->
    //     with(publication.pom) {
    //         withXml {
    //             val root = asNode()
    //             root.appendNode("name", "libui")
    //             root.appendNode("description", "Kotlin/Native interop to libui: a portable GUI library")
    //             root.appendNode("url", POM_SCM_URL)
    //         }
    //
    //         licenses {
    //             license {
    //                 name.set("MIT License")
    //                 url.set(POM_SCM_URL)
    //                 distribution.set("repo")
    //             }
    //         }
    //         developers {
    //             developer {
    //                 id.set("msink")
    //                 name.set("Mike Sinkovsky")
    //                 email.set("msink@permonline.ru")
    //             }
    //         }
    //         scm {
    //             url.set(POM_SCM_URL)
    //             connection.set(POM_SCM_CONNECTION)
    //             developerConnection.set(POM_SCM_DEV_CONNECTION)
    //         }
    //     }
    // }

    // publishing {
    //     publications {
    //         "$project.name"(MavenPublication) {
    //             customizePom(pom)
    //             groupId = "io.floodplain"
    //             artifactId = project.name
    //             version = rootProject.ext.floodplain_version
    //             from(components.java)
    //             artifact sourceJar
    //             artifact (javadocJar)
    //         }
    //     }
    //     repositories {
    //         maven {
    //             name = "Snapshots"
    //             url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
    //
    //             credentials {
    //                 username = project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")
    //                 password = project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")
    //             }
    //         }
    //         maven {
    //             name = "Staging"
    //             url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
    //
    //             credentials {
    //                 username = project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")
    //                 password = project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")
    //             }
    //         }
    //     }
    // }
//    signing {
//        sign publishing.publications."$project.name"(MavenPublication)
//    }
}

// fun customizePom(pom: MavenPom) {
//     pom.withXml {
//         val root = asNode()
//         // eliminate test-scoped dependencies (no need in maven central POMs)
//         root.dependencies.removeAll { dep ->
//             dep.scope == "test"
//         }
//
//         // add all items necessary for maven central publication
//         root.children().last() + {
//             resolveStrategy = Closure.DELEGATE_FIRST
//
//             description="Transforms CDC streams in Kotlin"
//             name="Floodplain"
//             url = "https://floodplain.io"
//             organization {
//                 name = "Floodplain"
//                 url = "https://floodplain.io"
//             }
//             issueManagement {
//                 system = "GitHub"
//                 url = "https://github.com/floodplainio/floodplain-library/issues"
//             }
//             licenses {
//                 license {
//                     name = "Apache License 2.0"
//                     url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
//                     distribution = "repo"
//                 }
//             }
//
//             scm {
//                 url = "https://github.com/floodplainio/floodplainio/floodplain-library"
//                 connection = "scm:git:git://github.com/floodplainio/floodplain-library.git"
//                 developerConnection = "scm:git:ssh://git@github.com:floodplainio/floodplain-library.git"
//             }
//             developers {
//                 developer {
//                     name = "Frank Lyaruu"
//                 }
//             }
//         }
//     }
// }
