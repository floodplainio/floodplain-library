import io.floodplain.build.FloodplainDeps
import io.floodplain.build.isReleaseVersion
import io.gitlab.arturbosch.detekt.Detekt

buildscript {
    repositories {
        mavenLocal()
        mavenCentral()
        gradlePluginPortal()
        jcenter()
    }
    dependencies {
        classpath("gradle.plugin.com.hierynomus.gradle.plugins:license-gradle-plugin:0.15.0")
        classpath("com.github.johnrengelman.shadow:com.github.johnrengelman.shadow.gradle.plugin:6.1.0")
        classpath("gradle.plugin.com.github.spotbugs.snom:spotbugs-gradle-plugin:4.5.1")
    }
}
plugins {
    id("eclipse")
    id("org.jetbrains.kotlin.jvm") version "1.4.20"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.4.20"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("org.jlleitschuh.gradle.ktlint") version "9.4.1"
    id("org.jetbrains.dokka") version "0.10.1"
    id("com.github.hierynomus.license-base").version("0.15.0")
    id("com.github.spotbugs") version "4.6.0"
    id("io.gitlab.arturbosch.detekt") version "1.15.0"
    signing
    `maven-publish`
    `java-library`
    jacoco
}

dependencies {
    implementation(io.floodplain.build.Libs.kotlin)
}

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

fun useSpotBugs(project: Project): Boolean {
    val kotlinSource = project.projectDir.resolve("src/main/kotlin")
    return !kotlinSource.exists()
}

subprojects {
    version = FloodplainDeps.floodplain_version
    apply(plugin = "java")
    apply(plugin = "eclipse")
    apply(plugin = "maven-publish")
    apply(plugin = "distribution")
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "com.github.hierynomus.license-base")
    apply(plugin = "jacoco")
    apply(plugin = "checkstyle")
    if (useSpotBugs(this)) {
        apply(plugin = "com.github.spotbugs")
    }
    jacoco {
        reportsDir = rootDir.resolve("jacocoReport").resolve(projectDir.name)
        reportsDir.mkdirs()
    }

    // tasks.
    tasks.test {
        finalizedBy(tasks.jacocoTestReport)
    }
    tasks.jacocoTestReport {
        dependsOn(tasks.test)
        reports {
            xml.isEnabled = true
            this.html.isEnabled = true
        }
    }

    tasks.withType<com.github.spotbugs.snom.SpotBugsTask>().configureEach {
        effort.set(com.github.spotbugs.snom.Effort.MAX)
        reports.maybeCreate("xml").isEnabled = false
        reports.maybeCreate("html").isEnabled = true
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
                "-Xopt-in=kotlinx.coroutines.FlowPreview",
                "-Xopt-in=kotlinx.coroutines.ExperimentalCoroutinesApi",
                "-Xopt-in=kotlin.time.ExperimentalTime"
            )
        }
    }

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
            dependsOn.add(dokka)
            archiveClassifier.set("dokka")
            from(dokka)
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
                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
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
        xml.enabled = false
        html.enabled = true
    }
}
