import io.floodplain.build.FloodplainDeps
import io.floodplain.build.isReleaseVersion
import io.gitlab.arturbosch.detekt.Detekt
import java.io.File

buildscript {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
        maven("https://plugins.gradle.org/m2/")
    }
    dependencies {
        classpath("gradle.plugin.com.hierynomus.gradle.plugins:license-gradle-plugin:0.16.1")
        // classpath("com.github.spotbugs.snom:spotbugs-gradle-plugin:5.0.6")
        classpath("gradle.plugin.com.github.spotbugs.snom:spotbugs-gradle-plugin:4.7.2")
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.18")
    }
}

val buildKotlinVersion: String by extra

plugins {
    kotlin("jvm") version "1.7.20"

    id("org.jetbrains.kotlin.plugin.allopen") version "1.7.20"
    id("org.jlleitschuh.gradle.ktlint") version "11.0.0"
    id("org.jetbrains.dokka") version "1.7.10"
    id("com.github.hierynomus.license-base").version("0.16.1")
    id("com.github.spotbugs") version "5.0.12"
    id("io.gitlab.arturbosch.detekt") version "1.21.0"
    signing
    `maven-publish`
    `java-library`
    jacoco
}

configurations.implementation {
    exclude(group = "org.apache.kafka", module = "kafka-log4j-appender")
    exclude(group = "log4j", module = "log4j")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

dependencies {
    implementation(io.floodplain.build.Libs.kotlin)
}

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
        google()
        maven {
            url = uri("https://packages.confluent.io/maven")
        }
    }
}

fun isKotlinModule(project: Project): Boolean {
    val kotlinMainSource = project.projectDir.resolve("src/main/kotlin")
    val kotlinTestSource = project.projectDir.resolve("src/test/kotlin")
    return kotlinMainSource.exists() || kotlinTestSource.exists()
}

subprojects {
    version = FloodplainDeps.floodplain_version
    apply(plugin = "java")
    apply(plugin = "eclipse")
    apply(plugin = "maven-publish")
    apply(plugin = "distribution")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "com.github.hierynomus.license-base")
    apply(plugin = "jacoco")
    // Crude exclusion for floodplain-direct, somehow seems to ignore exclude file TODO
    if (!isKotlinModule(this) && this.name != "floodplain-direct") {
        apply(plugin = "com.github.spotbugs")
        spotbugs {
            excludeFilter.set(rootProject.file("spotbugs-exclude.xml"))
        }
    }
    if (!isKotlinModule(this)) {
        // logger.warn("This project is not kotlin: ${this.name}")
        tasks.withType<com.github.spotbugs.snom.SpotBugsTask>().configureEach {
            // excludeFilter rootProject.file("spotbugs-exclude.xml")
            // excludeFilter.fileValue(rootProject.file("spotbugs-exclude.xml"))
            // baselineFile = file('spotbugs-baseline.xml')
            // val f: File = rootProject.file("spotbugs-exclude.xml")
            // excludeFilter.set(f)
            // effort.set(com.github.spotbugs.snom.Effort.MAX)
            reports.maybeCreate("xml").isEnabled = false
            reports.maybeCreate("html").isEnabled = true
        }
    }
    if (isKotlinModule(this)) {
        apply(plugin = "io.gitlab.arturbosch.detekt")
        detekt {
            // Version of Detekt that will be used. When unspecified the latest detekt
            // version found will be used. Override to stay on the same version.
            toolVersion = "1.19.0"

            // The directories where detekt looks for source files.
            // Defaults to `files("src/main/java", "src/main/kotlin")`.
            input = files("src/main/java", "src/main/kotlin")
            config = files("${rootDir.path}/detektConfig.yml")
        }
    }
    tasks.withType<com.hierynomus.gradle.license.tasks.LicenseFormat>().configureEach {
        this.header = File(this.project.rootDir, "HEADER")
        this.exclude("*.xml", "*.json")
        this.mapping(mapOf("java" to "SLASHSTAR_STYLE", "kt" to "SLASHSTAR_STYLE"))
    }

    tasks.test {
        useJUnitPlatform()
        jvmArgs("--enable-preview")
        finalizedBy(tasks.jacocoTestReport)
    }

    tasks.jacocoTestReport {
        dependsOn(tasks.test)
        classDirectories.setFrom(
            files(
                classDirectories.files.map {
                    fileTree(it) {
                        exclude("**/generated/**", "**/org/apache/**")
                    }
                }
            )
        )
    }

    tasks.withType<com.hierynomus.gradle.license.tasks.LicenseCheck>().configureEach {
        this.header = File(this.project.rootDir, "HEADER")
        this.exclude("*.xml", "*.json")
        this.mapping(mapOf("java" to "SLASHSTAR_STYLE", "kt" to "SLASHSTAR_STYLE"))
    }

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
                "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
                "-Xopt-in=kotlin.time.ExperimentalTime"
            )
        }
    }

    val dokkaHtml by tasks.getting(org.jetbrains.dokka.gradle.DokkaTask::class)

    tasks {
        val sourcesJar by creating(Jar::class) {
            archiveClassifier.set("sources")
            from(sourceSets.main.get().allSource)
        }

        val javadocJar by creating(Jar::class) {
            dependsOn.add(javadoc)
            archiveClassifier.set("javadoc")
            from(javadoc)
        }
        val dokkaJar by creating(Jar::class) {
            dependsOn.add(dokkaHtml)
            archiveClassifier.set("dokka")
            from(dokkaHtml.outputDirectory)
        }

        artifacts {
            archives(sourcesJar)
            archives(javadocJar)
            archives(dokkaJar)
            archives(jar)
        }
    }
    group = "io.floodplain"

    publishing {
        publications {
            create<MavenPublication>(project.name) {
                customizePom(this@create)
                groupId = "io.floodplain"
                artifactId = project.name
                version = FloodplainDeps.floodplain_version
                from(components["java"])
                val sourcesJar by tasks
                val javadocJar by tasks
                val dokkaJar by tasks

                artifact(sourcesJar)
                artifact(javadocJar)
                artifact(dokkaJar)
            }
        }
        repositories {
            maven {
                name = "Snapshots"
                url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
                credentials {
                    username = (project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME") ?: "") as String
                    password = (project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD") ?: "") as String
                }
            }
            maven {
                name = "Staging"
                url = uri("https://oss.sonatype.org/service/local/staging/deploy/maven2/")
                credentials {
                    username = (project.findProperty("gpr.user") ?: System.getenv("CENTRAL_USERNAME") ?: "") as String
                    password = (project.findProperty("gpr.key") ?: System.getenv("CENTRAL_PASSWORD") ?: "") as String
                }
            }
        }
    }

    apply(plugin = "signing")
    signing {
        if (isReleaseVersion()) {
            sign(publishing.publications[project.name])
        }
    }
}

fun customizePom(publication: MavenPublication) {
    with(publication.pom) {
        withXml {
            val root = asNode()
            root.appendNode("name", "Floodplain")
            root.appendNode("description", "Transforms CDC streams in Kotlin")
            root.appendNode("url", "https://floodplain.io")
        }
        organization {
            name.set("Floodplain")
            url.set("https://floodplain.io")
        }
        issueManagement {
            system.set("GitHub")
            url.set("https://github.com/floodplainio/floodplain-library/issues")
        }
        licenses {
            license {
                name.set("Apache License 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("flyaruu")
                name.set("Frank Lyaruu")
                email.set("flyaruu@gmail.com")
            }
        }
        scm {
            url.set("https://github.com/floodplainio/floodplainio/floodplain-library")
            connection.set("scm:git:git://github.com/floodplainio/floodplain-library.git")
            developerConnection.set("scm:git:ssh://git@github.com:floodplainio/floodplain-library.git")
        }
    }
}

val detektAll by tasks.registering(Detekt::class) {
    this.config.setFrom("${rootDir.path}/detektConfig.yml")
    description = "Runs over whole code base without the starting overhead for each module."
    parallel = true
    buildUponDefaultConfig = true
    setSource(files(projectDir))
    // this.setConfig(files(project.rootDir.resolve("reports/failfast.yml")))
    // config = files(project.rootDir.resolve("reports/failfast.yml"))
    include("**/*.kt")
    include("**/*.kts")
    exclude("**/resources/**")
    exclude("**/build/**")
    // baseline.set(project.rootDir.resolve("reports/baseline.xml"))
    reports {
        xml.enabled = true
        html.enabled = true
    }
}

jacoco {
    toolVersion = "0.8.7"
}

plugins.withType<JacocoPlugin> {
    val testTasks = tasks.withType<Test>()
    testTasks.configureEach {
        extensions.configure<JacocoTaskExtension> {
            // We don't want to collect coverage for third-party classes
            // includes?.add("**/io.floodplain.*.class")
            excludes?.add("**/generated/**")
            excludes?.add("**/org/apache/**")
        }
    }
}

tasks.withType<JacocoReport> {
    //     afterEvaluate {
//         classDirectories = files(sourceSets.getByName("main").output.files.collect {
//             fileTree(dir: it, exclude: ['**/*body*', '**/*inlined*'])
//         })
//     }

    // afterEvaluate {
    //     logger.warn("PRoject: ${this.name}")
    //     classDirectories.setFrom(fileTree("build/classes").apply {
    //         logger.warn("Tree: ${this.asPath}")
    //         exclude("**/generated/**")
    //     })
    // }
    // afterEvaluate {
    //     logger.warn("PRoject: ${this.name}")
    //     classDirectories.setFrom(fileTree("build/classes").apply {
    //         logger.warn("Tree: ${this.asPath}")
    //         exclude("**/generated/**")
    //     })
    // }
}

// tasks.jacocoTestCoverageVerification {
//     violationRules {
//         rule {
//             limit {
//                 minimum = "0.9".toBigDecimal()
//             }
//         }
//     }
//     classDirectories.setFrom(
//         sourceSets.main.get().output.asFileTree.matching {
//             // exclude main()
//             exclude("org/apache/kafka/streams/TopologyTestDriver.class")
//         }
//     )
// }

tasks.register<JacocoReport>("codeCoverageReport") {
    // If a subproject applies the 'jacoco' plugin, add the result it to the report
    val report = this
    subprojects {
        val subproject = this
        subproject.plugins.withType<JacocoPlugin>().configureEach {
            subproject.tasks.matching { it.extensions.findByType<JacocoTaskExtension>() != null }.configureEach {
                val testTask = this
                sourceSets(subproject.sourceSets.main.get())
                // executionData(fileTree(project.rootDir.absolutePath).include("**/build/jacoco/*.exec"))
                val execFiles = files(testTask).filter { it.exists() && it.name.endsWith(".exec") }
                executionData(execFiles)
            }
            report.classDirectories.setFrom(
                files(
                    classDirectories.files.map {
                        fileTree(it) {
                            exclude("**/generated/**", "**/org/apache/**", "**/io/floodplain/kotlindsl/example/**")
                        }
                    }
                )
            )
            // To automatically run `test` every time `./gradlew codeCoverageReport` is called,
            // you may want to set up a task dependency between them as shown below.
            // Note that this requires the `test` tasks to be resolved eagerly (see `forEach`) which
            // may have a negative effect on the configuration time of your build.
            subproject.tasks.matching { it.extensions.findByType<JacocoTaskExtension>() != null }.forEach {

                rootProject.tasks["codeCoverageReport"].dependsOn(it)
            }
        }
    }

    // enable the different report types (html, xml, csv)
    reports {
        // xml is usually used to integrate code coverage with
        // other tools like SonarQube, Coveralls or Codecov
        xml.required.set(true)

        // HTML reports can be used to see code coverage
        // without any external tools
        html.required.set(true)
    }
}

tasks.withType<Test> {
    useJUnitPlatform {
        includeEngines = setOf("junit-jupiter")
        // this.includeTags = setOf("*")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
    configure<JacocoTaskExtension> {
        isEnabled = true
        includes = listOf("**/io.floodplain.*.class")
    }
}
