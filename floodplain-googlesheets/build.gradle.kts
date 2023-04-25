import io.floodplain.build.FloodplainDeps
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.20"
}

// plugins {
//     id("com.github.johnrengelman.shadow")
// }

dependencies {
    implementation(FloodplainDeps.kotlinLogging)
    implementation(project(":floodplain-stream-topology"))
    implementation("org.apache.commons:commons-math3:3.6.1")

    implementation("com.google.guava:guava:30.1.1-jre")

    implementation("com.google.oauth-client:google-oauth-client-jetty:1.31.5")
    implementation("com.google.apis:google-api-services-sheets:v4-rev612-1.25.0")
    implementation("org.apache.kafka:connect-api:${FloodplainDeps.kafka_version}")
    implementation("${FloodplainDeps.jacksonCore}")
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation("org.junit.jupiter:junit-jupiter:${FloodplainDeps.junit_5_version}")
    implementation(kotlin("stdlib-jdk8"))
}

// tasks {
//     named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
//         archiveBaseName.set("shadow")
//         mergeServiceFiles()
//         exclude("org.apache.kafka:connect-api:*")
//         exclude("org.apache.kafka:kafka-clients:*")
//         exclude("net.jpountz.lz4:.*:.*")
//         exclude("org.xerial.snappy:.*:.*")
//         exclude("org.slf4j:.*:.*")
//         // manifest {
//         //     attributes(mapOf("Main-Class" to "com.github.csolem.gradle.shadow.kotlin.example.App"))
//         // }
//     }
// }

// tasks {
//     build {
//         dependsOn(shadowJar)
//     }
// }
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "17"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "17"
}