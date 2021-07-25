import io.floodplain.build.FloodplainDeps

// plugins {
//     id("com.github.johnrengelman.shadow")
// }

dependencies {
    compile(FloodplainDeps.kotlinLogging)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(project(":floodplain-stream-topology"))
    // This dependency is exported to consumers, that is to say found on their compile classpath.
    implementation("org.apache.commons:commons-math3:3.6.1")

    // This dependency is used internally, and not exposed to consumers on their own compile classpath.
    implementation("com.google.guava:guava:30.1.1-jre")

    implementation("com.google.oauth-client:google-oauth-client-jetty:1.31.5")
    implementation("com.google.apis:google-api-services-sheets:v4-rev612-1.25.0")
    implementation("org.apache.kafka:connect-api:2.8.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.2")
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-stream-topology"))
    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(FloodplainDeps.kotlinCoroutines)
    testImplementation(FloodplainDeps.junitJupiterApi)
    testImplementation(FloodplainDeps.junitJupiterParams)
    testRuntimeOnly(FloodplainDeps.junitJupiterEngine)
    testRuntimeOnly(FloodplainDeps.junitJupiterVintage)
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
