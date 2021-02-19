import io.floodplain.build.FloodplainDeps

plugins {
    // Apply the java-library plugin to add support for Java Library
    id("java")
    id("application")
}

dependencies {
    implementation(project(":floodplain-dsl"))
    implementation(project(":floodplain-mongodb"))
    implementation(project(":floodplain-googlesheets"))
    implementation(project(":floodplain-elasticsearch"))
    implementation(project(":floodplain-fhir"))
    implementation("ca.uhn.hapi.fhir:hapi-fhir-base:5.2.0")
    implementation("ca.uhn.hapi.fhir:hapi-fhir-structures-r4:5.2.0")
    implementation("ca.uhn.hapi.fhir:org.hl7.fhir.r4:5.2.0")

    implementation(project(":streams-api"))
    implementation(project(":streams"))
    implementation(project(":replication-api"))
    implementation(project(":replication-json"))
    implementation("io.confluent:kafka-connect-elasticsearch:5.5.0")
    implementation(FloodplainDeps.kotlinCoroutines)
    compile(FloodplainDeps.slf4j)
    testCompile(FloodplainDeps.jUnit)
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
}
// shadowJar {
//     mergeServiceFiles {
//         path = "META-INF/custom"
//         exclude "META-INF/services/javax.xml.stream.*"
//     }
// }
