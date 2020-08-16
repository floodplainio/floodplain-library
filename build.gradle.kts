import io.floodplain.build.FloodplainDeps

buildscript {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
        jcenter()
    }
    dependencies {
        classpath("gradle.plugin.com.hierynomus.gradle.plugins:license-gradle-plugin:0.15.0")
    }
}
plugins {
    id("eclipse")
    id("org.jetbrains.kotlin.jvm") version "1.3.72"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.3.72"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("org.jlleitschuh.gradle.ktlint") version "9.2.1"
    id("com.palantir.graal") version "0.7.0-5-g838c2ab"
    id("org.jetbrains.dokka") version "0.10.1"
    id("com.github.hierynomus.license-base").version("0.15.0")
    signing
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
}

subprojects {
    version = FloodplainDeps.floodplain_version
    apply(plugin = "java")
    apply(plugin = "eclipse")
    apply(plugin = "maven-publish")
    apply(plugin = "distribution")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")

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
    // configure<LicenseExtension> {
    //     header = file("$rootDir/gradle/LICENSE_HEADER.txt")
    //     skipExistingHeaders = false
    //     val settings = HashMap<String, String>()
    //     val exclude = ArrayList<String>()
    //     exclude.add("**/*.json")
    //     settings.put("java", "SLASHSTAR_STYLE")
    //     mapping(
    //         settings
    //     )
    //     excludes(
    //         exclude
    //     )
    // }

    // apply(plugin = "com.github.hierynomus.license-base")

    apply(plugin = "signing")
    group = "io.floodplain"

    signing {
        val signingKey: String? by project
        val signingPassword: String? by project
        useInMemoryPgpKeys(signingKey, signingPassword)
       // sign publishing.publications."$project.name"(MavenPublication)
    }
    publishing {
        publications {
            // register("mavenJava", MavenPublication::class) {
            //     from(components["java"])
            //     artifact(sourcesJar.get())
            // }
            create<MavenPublication>("mavenJava") {
                // customizePom(pom)
                groupId = "io.floodplain"
                artifactId = project.name
                version = FloodplainDeps.floodplain_version
                // artifact(sourcesJar.get())
                // artifact (javadocJar)
            }
        }
        repositories {
            maven {
                name = "Snapshots"
                url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
                credentials {
                    username = (project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")) as String
                    password = (project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")) as String
                }
            }
            maven {
                name = "Staging"
                url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = (project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME")) as String
                    password = (project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD")) as String
                }
            }
        }
    }
}

val sourcesJar by tasks.registering(Jar::class) {
    classifier = "sources"
    from(sourceSets.main.get().allSource)
}

// fun customizePom(pom: MavenPom) {
//     pom.withXml {
//         val root: Node = asNode()
//         // eliminate test-scoped dependencies (no need in maven central POMs)
//         // root.
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
